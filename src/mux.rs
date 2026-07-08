//! Multiplexer -- manages sessions over a single RNS link.
//!
//! All TCP sessions are multiplexed over one RNS link per peer. Each session
//! gets a unique `session_id`. Data is exchanged as frames:
//!
//! ```text
//! [1 byte type][4 bytes session_id][2 bytes payload length][payload]
//! ```
//!
//! Frames are sent as raw encrypted link data via `send_on_link` (CONTEXT_NONE).
//! Frames larger than LINK_MDU are split into LINK_MDU-sized chunks; the
//! receiver reassembles them using the frame's embedded length header.
//!
//! We do NOT use the RNS Channel API because its ACK path is not wired up
//! in rns-rs, causing `NotReady` errors after the first 2 messages.

use std::collections::HashMap;
use std::error;
use std::sync::{Arc, Mutex};

use log::{debug, error, info, warn};
use rns_core::constants::LINK_MDU;
use rns_core::msgpack::Error;
use rns_net::{LinkId, LocalServerFactory, RnsNode};
use tokio::sync::mpsc;

use crate::frame::FrameDecodeState::{DecodingFailed, Finished, MoreDataRequired};
use crate::{Frame, FrameType};

/// Context byte for our link data. We use CONTEXT_NONE (0x00) which routes
/// to the `on_link_data` callback on the receive side.
const DATA_CONTEXT: u8 = 0x00;

/// A handle that session tasks use to send frames back through the RNS link.
#[derive(Clone)]
pub struct MuxHandle {
    inner: Arc<MuxInner>,
}

struct MuxInner {
    node: Arc<RnsNode>,
    link_id: Mutex<Option<LinkId>>,
    sessions: Mutex<HashMap<u32, mpsc::UnboundedSender<Frame>>>,
    next_sid: Mutex<u32>,
    /// Reassembly buffer for incoming raw link data chunks.
    recv_buf: Mutex<Vec<u8>>,
}

impl MuxHandle {
    /// Create a new multiplexer handle.
    pub fn new(node: Arc<RnsNode>) -> Self {
        Self {
            inner: Arc::new(MuxInner {
                node,
                link_id: Mutex::new(None),
                sessions: Mutex::new(HashMap::new()),
                next_sid: Mutex::new(0),
                recv_buf: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Set the active link id (called when the link is established).
    pub fn set_link_id(&self, link_id: LinkId) {
        *self.inner.link_id.lock().unwrap() = Some(link_id);
    }

    /// Clear the link id (called when the link is closed).
    pub fn clear_link_id(&self) {
        *self.inner.link_id.lock().unwrap() = None;
    }

    /// Reset the mux for a new link (clears sessions and reassembly buffer).
    ///
    /// Called on reconnection.
    pub fn reset(&self) {
        *self.inner.link_id.lock().unwrap() = None;
        self.inner.sessions.lock().unwrap().clear();
        self.inner.recv_buf.lock().unwrap().clear();
    }

    /// Check if link is active.
    pub fn is_connected(&self) -> bool {
        self.inner.link_id.lock().unwrap().is_some()
    }

    /// Get the next session id.
    pub fn next_session_id(&self) -> u32 {
        let mut sid = self.inner.next_sid.lock().unwrap();
        *sid = sid.wrapping_add(1);
        *sid
    }

    /// Register a session. Returns a receiver for frames addressed to this session.
    pub fn register_session(&self, sid: u32) -> mpsc::UnboundedReceiver<Frame> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.inner.sessions.lock().unwrap().insert(sid, tx);
        rx
    }

    /// Remove a session.
    pub fn drop_session(&self, sid: u32) {
        self.inner.sessions.lock().unwrap().remove(&sid);
    }

    /// Send a frame over the RNS link.
    ///
    /// The encoded frame is split into chunks of at most `LINK_MDU` bytes and
    /// each chunk is sent as a separate `send_on_link` call with `CONTEXT_NONE`.
    /// The receiver reassembles using the frame length header.
    ///
    /// Note that we are using .inner.link_id.lock as the guard to prevent
    /// multiple different sids from sending at the same time and scrambling packets
    pub fn send_frame(&self, frame: &Frame) {
        let lock = self.inner.link_id.lock().unwrap();
        let value = &*(lock);
        let link_id = match value {
            Some(id) => id,
            None => {
                debug!("send_frame: no active link, dropping frame");
                return;
            }
        };

        let encoded = frame.encode();
        let node = &self.inner.node;

        // Send in LINK_MDU-sized chunks

        for chunk in encoded.chunks(LINK_MDU) {
            println!("chunk: {:?} sid: {}",chunk, frame.session_id);
            if let Err(e) = node.send_on_link(link_id.0, chunk.to_vec(), DATA_CONTEXT) {
                warn!("Failed to send link data: {:?}", e);
                return;
            }
        }
        drop(lock);
    }

    /// Convenience: send a typed frame.
    pub fn send(&self, frame_type: FrameType, session_id: u32, payload: Vec<u8>) {
        info!("send frame");
        self.send_frame(&Frame::new(frame_type, session_id, payload));
    }

    /// Dispatch an incoming frame to the appropriate session.
    pub fn dispatch(&self, frame: Frame) {
        let sid = frame.session_id;
        let ft = frame.frame_type;
        let sessions = self.inner.sessions.lock().unwrap();
        if let Some(tx) = sessions.get(&sid) {
            if tx.send(frame).is_err() {
                debug!("Session {} channel closed", sid);
            }
        } else {
            warn!("No session {} for frame type {}", sid, ft);
        }
    }

    /// Feed raw link data and extract any complete frames.
    ///
    /// Called when `on_link_data` fires. The data may be a partial chunk of a
    /// larger frame, so we buffer and try to decode complete frames.
    pub fn receive_data(&self, data: &[u8]) -> Vec<Frame> {
        let mut buf = self.inner.recv_buf.lock().unwrap();
        buf.extend_from_slice(data);
            info!("buf: {:?}", buf);

        let mut frames = Vec::new();
        loop {
            match Frame::decode(&buf) {
                Ok((frame, consumed)) => {
                    info!("consume {}",consumed);
                    buf.drain(..consumed);
                    frames.push(frame);
                }
                Err(err) => {
                    match err {
                        Finished => {
                            // finished decoding all packets basically
                            // though there might still be like 5 bytes left in the buffer
                            info!("finished");
                            break;
                        }
                        MoreDataRequired => {
                            info!("more data required");
                            break;
                           // just wait for next packet 
                        }
                        DecodingFailed => {
                            // something has gone really wrong, just clear the buffer and hope things
                            // work out.
                            error!("decoding failed for a packet, something really bad is happening {:?}",buf);
                            error!("ignoring packet buffer and hoping that will fix it");
                            buf.drain(..);
                            break;
                        }
                    }
                },
            }
        }

        info!("frames {:?}", &frames);
        if buf.len() > 9000 {
            warn!("buf thing: {:?}", &buf);
            assert!(false);
        }
        frames
    }
}

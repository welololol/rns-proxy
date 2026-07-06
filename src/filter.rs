///! Socket Filter
///! 
///! This allows the server side client to restrict what ips are allowed. as well as what ports 
///! 
///! This allows for generals socksv5 proxying while for example disabling accessing the localhost or computer's private network or disabling all UDP.
///! but can also be used for more restrictive purposes like only allowing clients to connect to a certain localhost.
///!
///! # Filter list  
///! the filter list consists of a list of filters which can either be include or exclude
///! with the top having the least prioty and more granular settings and the bottom having the most priorty. 
///! 
///! by defeault an addressj
///! 
///! # examples
///! ## example 1, only allow dns requests
///!  All:53:udp Include
///! ## example 2, allow connections only to a local IRC chat 
///!  All:194 Include
///! ## example 3, generic sockets server that blocks udp, private and localhost except for a localhost web server
///!  All Include
///!  Private Exclude
///!  Localhost Exclude
///!  Localhost:80:tcp
///!
struct a {
    
}

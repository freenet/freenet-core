#![allow(unused_variables, dead_code)]

/*
 NOTES

Distribution of connections should mirror the distribution of inbound and locally generated requests.

For outbound join requests this means random selection of outbound requests in proportion to outbound request density.

For inbound requests this means selecting the inbound request in the highest density region of the keyspace.

a - 100 requests / sec
b - 50 requests / sec

Distance: 0.01

Density at point c between a and b = ((distance(a, c) * req(b) + distance(b, c) * req(a)) / (distance(a, b)) / distance(a, b)
distance(a, b) cancel out


*/

mod sliding_window;
mod small_world_rand;

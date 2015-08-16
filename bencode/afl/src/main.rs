#![feature(plugin)]
#![plugin(afl_coverage_plugin)]

extern crate afl_coverage;
extern crate bencode;

fn main() {
	bencode::afl::bdecode();
}

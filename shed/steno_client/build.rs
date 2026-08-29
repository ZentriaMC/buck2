use std::io;

fn main() -> io::Result<()> {
    let builder = buck2_protoc_dev::configure();
    unsafe { builder.setup_protoc() }.compile(&["steno.proto"], &["."])
}

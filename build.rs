use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/skyvault/v1/skyvault.proto")?;

    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    tonic_build::configure()
        .file_descriptor_set_path(out_dir.join("skyvault_descriptor.bin"))
        .compile_protos(
            &["proto/skyvault/v1/skyvault.proto"],
            &["proto/skyvault/v1"],
        )?;

    Ok(())
}

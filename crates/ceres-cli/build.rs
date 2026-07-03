use vergen_gitcl::{Build, Cargo, Emitter, Gitcl, Rustc};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Build instructions
    let git = Gitcl::builder().sha(true).build();
    let build = Build::builder().build_date(true).build();
    let cargo = Cargo::builder().target_triple(true).build();
    let rustc = Rustc::builder().semver(true).build();

    // Emit env variables for cargo
    Emitter::default()
        .add_instructions(&build)?
        .add_instructions(&cargo)?
        .add_instructions(&git)?
        .add_instructions(&rustc)?
        .emit()?;

    Ok(())
}

use vergen_git2::{BuildBuilder, CargoBuilder, Emitter, Git2Builder, RustcBuilder};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Build instructions
    let git2 = Git2Builder::default().sha(true).build()?;
    let build = BuildBuilder::default().build_date(true).build()?;
    let cargo = CargoBuilder::default().target_triple(true).build()?;
    let rustc = RustcBuilder::default().semver(true).build()?;

    // Emit env variables for cargo
    Emitter::default()
        .add_instructions(&build)?
        .add_instructions(&cargo)?
        .add_instructions(&git2)?
        .add_instructions(&rustc)?
        .emit()?;

    Ok(())
}

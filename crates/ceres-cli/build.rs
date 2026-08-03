use vergen_gitcl::{Build, Cargo, Emitter, Gitcl, Rustc};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-env-changed=CERES_GIT_SHA");

    // Build instructions
    let build = Build::builder().build_date(true).build();
    let cargo = Cargo::builder().target_triple(true).build();
    let rustc = Rustc::builder().semver(true).build();

    let mut emitter = Emitter::default();
    emitter
        .add_instructions(&build)?
        .add_instructions(&cargo)?
        .add_instructions(&rustc)?;

    if let Ok(sha) = std::env::var("CERES_GIT_SHA") {
        let valid = sha == "unknown"
            || ((7..=64).contains(&sha.len()) && sha.chars().all(|c| c.is_ascii_hexdigit()));
        if !valid {
            return Err(
                "CERES_GIT_SHA must be 'unknown' or a 7-64 character hexadecimal SHA".into(),
            );
        }
        println!("cargo:rustc-env=VERGEN_GIT_SHA={sha}");
    } else {
        // Local builds can discover the commit directly. Container builds inject
        // CERES_GIT_SHA because the intentionally small build context omits .git.
        let git = Gitcl::builder().sha(true).build();
        emitter.add_instructions(&git)?;
    }

    emitter.emit()?;

    Ok(())
}

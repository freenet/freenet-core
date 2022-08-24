use std::{
    borrow::Cow,
    env,
    fs::{self, File},
    io::{self, Cursor, Read, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};

use locutus_runtime::locutus_stdlib::web::{controller::ControllerState, view::WebViewState};
use serde::Deserialize;
use tar::Builder;

use crate::{ContractType, DynError, PackageManagerConfig};

const DEFAULT_OUTPUT_NAME: &str = "contract-state";

// TODO: polish error handling with its own error type
#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("Configuration error: {0}")]
    MissConfiguration(Cow<'static, str>),
    #[error("Command failed: {0}")]
    CommandFailed(&'static str),
}

pub fn build_package(_cli_config: PackageManagerConfig) -> Result<(), DynError> {
    let cwd = env::current_dir()?;
    let config_file = cwd.join("locutus.toml");
    if config_file.exists() {
        let mut f_content = vec![];
        File::open(config_file)?.read_to_end(&mut f_content)?;
        let config: BuildToolConfig = toml::from_slice(&f_content)?;
        internal_package_state(config, &cwd)
    } else {
        Err("could not locate `locutus.toml` config file in current dir".into())
    }
}

#[derive(Deserialize)]
struct BuildToolConfig {
    package: Package,
    sources: Sources,
    metadata: Option<PathBuf>,
    output: Option<Output>,
    view: Option<ViewContract>,
}

#[derive(Deserialize)]
struct Package {
    #[serde(rename(deserialize = "type"))]
    c_type: ContractType,
    lang: Option<SupportedContractLangs>,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum SupportedContractLangs {
    Rust,
}

#[derive(Deserialize)]
struct Sources {
    source_dirs: Option<Vec<PathBuf>>,
    files: Option<Vec<String>>,
}

#[derive(Deserialize)]
struct Output {
    path: PathBuf,
}

#[derive(Deserialize)]
struct ViewContract {
    lang: SupportedViewLangs,
    typescript: Option<TypescriptConfig>,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum SupportedViewLangs {
    Javascript,
    Typescript,
}

#[derive(Deserialize)]
struct TypescriptConfig {
    #[serde(default)]
    webpack: bool,
}

fn internal_package_state(mut config: BuildToolConfig, cwd: &Path) -> Result<(), DynError> {
    let metadata = if let Some(md) = &config.metadata {
        let mut buf = vec![];
        File::open(md)?.read_to_end(&mut buf)?;
        buf
    } else {
        vec![]
    };

    compile_contract(&config, cwd)?;

    match config.package.c_type {
        ContractType::View => build_view_state(metadata, &config, cwd)?,
        ContractType::Controller => build_controller_state(metadata, &mut config)?,
    }

    if let Some(_lang) = &config.package.lang {
        // todo: try to compile the package
    }
    Ok(())
}

fn build_view_state(
    metadata: Vec<u8>,
    config: &BuildToolConfig,
    cwd: &Path,
) -> Result<(), DynError> {
    println!("Bundling `view` contract state");
    let mut archive: Builder<Cursor<Vec<u8>>> = Builder::new(Cursor::new(Vec::new()));
    let mut found_entry = false;

    if let Some(view_config) = &config.view {
        match &view_config.lang {
            SupportedViewLangs::Typescript => {
                let web_dir = cwd.join("web");
                if web_dir.exists() {
                    let webpack = view_config
                        .typescript
                        .as_ref()
                        .map(|c| c.webpack)
                        .unwrap_or_default();
                    if webpack {
                        let cmd_args: &[&str] =
                            if atty::is(atty::Stream::Stdout) && atty::is(atty::Stream::Stderr) {
                                &["--color"]
                            } else {
                                &[]
                            };
                        let child = Command::new("webpack")
                            .args(cmd_args)
                            .current_dir(web_dir)
                            .stdout(Stdio::piped())
                            .stderr(Stdio::piped())
                            .spawn()
                            .map_err(|e| {
                                eprintln!("Error while executing webpack command: {e}");
                                Error::CommandFailed("tsc")
                            })?;
                        pipe_std_streams(child)?;
                        println!("Compiled input using webpack");
                    } else {
                        let cmd_args: &[&str] =
                            if atty::is(atty::Stream::Stdout) && atty::is(atty::Stream::Stderr) {
                                &["--pretty"]
                            } else {
                                &[]
                            };
                        let child = Command::new("tsc")
                            .args(cmd_args)
                            .current_dir(web_dir)
                            .stdout(Stdio::piped())
                            .stderr(Stdio::piped())
                            .spawn()
                            .map_err(|e| {
                                eprintln!("Error while executing command tsc: {e}");
                                Error::CommandFailed("tsc")
                            })?;
                        pipe_std_streams(child)?;
                        println!("Compiled input using tsc");
                    }
                } else {
                    println!("`web` dir not written, skipping compiling web dependencies");
                }
            }
            SupportedViewLangs::Javascript => todo!(),
        }
    }

    if let Some(sources) = &config.sources.files {
        for src in sources {
            for entry in glob::glob(src)? {
                let p = entry?;
                if p.ends_with("index.html") && p.starts_with("index.html") {
                    // ensures that index is present and at the root
                    found_entry = true;
                }
                let mut f = File::open(&p)?;
                archive.append_file(p, &mut f)?;
            }
        }
    }
    if let Some(src_dirs) = &config.sources.source_dirs {
        for dir in src_dirs {
            if dir.is_dir() {
                let present_entry = dir.join("index.html").exists();
                if !found_entry && present_entry {
                    found_entry = true;
                } else if present_entry {
                    return Err(format!(
                        "duplicate entry point (index.html) found at directory: {dir:?}"
                    )
                    .into());
                }
                archive.append_dir_all(".", &dir)?;
            } else {
                return Err(format!("unknown directory: {dir:?}").into());
            }
        }
    }

    if config.sources.source_dirs.is_none() && config.sources.files.is_none() {
        return Err("need to specify source dirs and/or files".into());
    }
    if !found_entry {
        return Err("didn't find entry point `index.html` in package".into());
    } else {
        let state = WebViewState::from_data(metadata, archive)?;
        let packed = state.pack()?;
        output_artifact(&config.output, &packed)?;
        println!("Finished bundling `view` contract state");
    }

    Ok(())
}

fn build_controller_state(metadata: Vec<u8>, config: &mut BuildToolConfig) -> Result<(), DynError> {
    const REQ_ONE_FILE_ERR: &str = "Requires exactly one source file specified for the state.";

    println!("Bundling `controller` contract state");
    let src_files = config
        .sources
        .files
        .as_mut()
        .ok_or_else(|| Error::MissConfiguration(REQ_ONE_FILE_ERR.into()))?;

    let state: PathBuf = (src_files.len() == 1)
        .then(|| src_files.pop().unwrap())
        .ok_or_else(|| Error::MissConfiguration(REQ_ONE_FILE_ERR.into()))?
        .into();

    let mut buf = vec![];
    let mut model_f = File::open(state)?;
    model_f.read_to_end(&mut buf)?;
    let controller_state = ControllerState::from_data(metadata, buf);
    let packed = controller_state.pack()?;
    output_artifact(&config.output, &packed)?;
    println!("Finished bundling `controller` contract state");
    Ok(())
}

#[inline]
fn get_default_ouput_dir() -> std::io::Result<PathBuf> {
    let output = env::current_dir()?.join("build").join("locutus");
    fs::create_dir_all(&output)?;
    Ok(output)
}

fn output_artifact(output: &Option<Output>, packed: &[u8]) -> Result<(), DynError> {
    if let Some(output) = output {
        File::create(&output.path)?.write_all(packed)?;
    } else {
        let default_out_dir = get_default_ouput_dir()?;
        fs::create_dir_all(&default_out_dir)?;
        let mut f = File::create(default_out_dir.join(DEFAULT_OUTPUT_NAME))?;
        f.write_all(packed)?;
    }
    Ok(())
}

fn compile_contract(config: &BuildToolConfig, cwd: &Path) -> Result<(), DynError> {
    let work_dir = match config.package.c_type {
        ContractType::View => cwd.join("container"),
        ContractType::Controller => cwd.to_path_buf(),
    };
    match config.package.lang {
        Some(SupportedContractLangs::Rust) => {
            const RUST_TARGET_ARGS: &[&str] =
                &["build", "--release", "--target", "wasm32-unknown-unknown"];
            const ERR: &str = "Cargo.toml definition incorrect";
            let cmd_args = if atty::is(atty::Stream::Stdout) && atty::is(atty::Stream::Stderr) {
                RUST_TARGET_ARGS
                    .iter()
                    .copied()
                    .chain(["--color", "always"])
                    .collect::<Vec<_>>()
            } else {
                RUST_TARGET_ARGS.to_vec()
            };

            println!("Compiling contract with rust");
            let child = Command::new("cargo")
                .args(&cmd_args)
                .current_dir(&work_dir)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(|e| {
                    eprintln!("Error while executing cargo command: {e}");
                    Error::CommandFailed("tsc")
                })?;
            pipe_std_streams(child)?;

            let mut f_content = vec![];
            File::open(work_dir.join("Cargo.toml"))?.read_to_end(&mut f_content)?;
            let cargo_config: toml::Value = toml::from_slice(&f_content)?;
            let package_name = cargo_config
                .as_table()
                .ok_or_else(|| Error::MissConfiguration(ERR.into()))?
                .get("package")
                .ok_or_else(|| Error::MissConfiguration(ERR.into()))?
                .as_table()
                .ok_or_else(|| Error::MissConfiguration(ERR.into()))?
                .get("name")
                .ok_or_else(|| Error::MissConfiguration(ERR.into()))?
                .as_str()
                .ok_or_else(|| Error::MissConfiguration(ERR.into()))?
                .replace('-', "_");
            let mut output_lib = env::var("CARGO_TARGET_DIR")?
                .parse::<PathBuf>()?
                .join("wasm32-unknown-unknown")
                .join("release")
                .join(&package_name);
            output_lib.set_extension("wasm");

            if !output_lib.exists() {
                return Err(Error::MissConfiguration(
                    format!("couldn't find output file: {output_lib:?}").into(),
                )
                .into());
            }
            let mut out_file = if let Some(output) = &config.output {
                output.path.join(package_name)
            } else {
                get_default_ouput_dir()?.join(package_name)
            };
            out_file.set_extension("wasm");
            fs::copy(output_lib, out_file)?;
        }
        None => println!("no lang specified, skipping contract compilation"),
    }
    println!("Contract compiled");
    Ok(())
}

fn pipe_std_streams(mut child: Child) -> Result<(), DynError> {
    let mut c_stdout = child.stdout.take().expect("Failed to open command stdout");
    let mut stdout = io::stdout();
    let mut stdout_buf = vec![];

    let mut c_stderr = child.stderr.take().expect("Failed to open command stderr");
    let mut stderr = io::stderr();
    let mut stderr_buf = vec![];

    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if !status.success() {
                    return Err(format!("exist with status: {status}").into());
                }
                break;
            }
            Ok(None) => {
                // attempt to write output to parent stds
                c_stdout.read_to_end(&mut stdout_buf)?;
                stdout.write_all(&stdout_buf)?;
                stdout_buf.clear();

                c_stderr.read_to_end(&mut stderr_buf)?;
                stderr.write_all(&stderr_buf)?;
                stderr_buf.clear();
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(err) => return Err(err.into()),
        }
    }

    // write out any remaining input
    c_stdout.read_to_end(&mut stdout_buf)?;
    stdout.write_all(&stdout_buf)?;
    stdout_buf.clear();

    c_stderr.read_to_end(&mut stderr_buf)?;
    stderr.write_all(&stderr_buf)?;
    stderr_buf.clear();

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    fn setup_view_contract() -> Result<(BuildToolConfig, PathBuf), DynError> {
        const CRATE_DIR: &str = env!("CARGO_MANIFEST_DIR");
        let cwd = PathBuf::from(CRATE_DIR).join("../../apps/freenet-microblogging/view");
        env::set_current_dir(&cwd)?;
        Ok((
            BuildToolConfig {
                sources: Sources {
                    source_dirs: Some(vec!["web/dist".into()]),
                    files: None,
                },
                package: Package {
                    c_type: ContractType::View,
                    lang: Some(SupportedContractLangs::Rust),
                },
                metadata: None,
                output: None,
                view: Some(ViewContract {
                    lang: SupportedViewLangs::Typescript,
                    typescript: Some(TypescriptConfig { webpack: true }),
                }),
            },
            cwd,
        ))
    }

    #[test]
    fn package_view_state() -> Result<(), DynError> {
        let (config, cwd) = setup_view_contract()?;
        build_view_state(vec![], &config, &cwd)?;

        let mut buf = vec![];
        File::open(
            PathBuf::from("target")
                .join("locutus")
                .join(DEFAULT_OUTPUT_NAME),
        )?
        .read_to_end(&mut buf)?;
        let state = locutus_runtime::locutus_stdlib::interface::State::from(buf);
        let mut view = WebViewState::try_from(state.as_ref()).unwrap();

        let target = env::temp_dir().join("locutus-unpack-state");
        let e = view.unpack(&target);
        let unpacked_successfully = target.join("index.html").exists();

        fs::remove_dir_all(target)?;
        e?;
        assert!(unpacked_successfully, "failed to unpack state");

        Ok(())
    }

    #[test]
    fn compile_view_contract() -> Result<(), DynError> {
        let (config, cwd) = setup_view_contract()?;
        compile_contract(&config, &cwd)?;
        Ok(())
    }

    #[test]
    fn package_controller_state() -> Result<(), DynError> {
        const CRATE_DIR: &str = env!("CARGO_MANIFEST_DIR");
        let cwd = PathBuf::from(CRATE_DIR).join("../../apps/freenet-microblogging/controller");
        env::set_current_dir(&cwd)?;
        let mut config = BuildToolConfig {
            sources: Sources {
                source_dirs: None,
                files: Some(vec!["initial_state.json".into()]),
            },
            package: Package {
                c_type: ContractType::Controller,
                lang: Some(SupportedContractLangs::Rust),
            },
            metadata: None,
            output: None,
            view: None,
        };

        build_controller_state(vec![], &mut config)?;

        assert!(PathBuf::from("target")
            .join("locutus")
            .join(DEFAULT_OUTPUT_NAME)
            .exists());

        Ok(())
    }
}

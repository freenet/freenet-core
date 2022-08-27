use std::{
    borrow::Cow,
    env,
    fs::{self, File},
    io::{self, Cursor, Read, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};

use locutus_runtime::locutus_stdlib::web::WebApp;
use serde::{Deserialize, Serialize};
use tar::Builder;

use crate::{config::BuildToolCliConfig, DynError};

const DEFAULT_OUTPUT_NAME: &str = "contract-state";

// TODO: polish error handling with its own error type
#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("Configuration error: {0}")]
    MissConfiguration(Cow<'static, str>),
    #[error("Command failed: {0}")]
    CommandFailed(&'static str),
}

pub fn build_package(_cli_config: BuildToolCliConfig) -> Result<(), DynError> {
    let cwd = env::current_dir()?;
    let config_file = cwd.join("locutus.toml");
    if config_file.exists() {
        let mut f_content = vec![];
        File::open(config_file)?.read_to_end(&mut f_content)?;
        let mut config: BuildToolConfig = toml::from_slice(&f_content)?;

        compile_contract(&config, &cwd)?;

        match config.contract.c_type.unwrap_or(ContractType::Standard) {
            ContractType::WebApp => build_web_state(&config)?,
            ContractType::Standard => build_generic_state(&mut config)?,
        }

        if let Some(_lang) = &config.contract.lang {
            // todo: try to compile the package
        }
        Ok(())
    } else {
        Err("could not locate `locutus.toml` config file in current dir".into())
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct BuildToolConfig {
    pub contract: Contract,
    pub state: Option<Sources>,
    pub webapp: Option<WebAppContract>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Sources {
    pub source_dirs: Option<Vec<PathBuf>>,
    pub files: Option<Vec<String>>,
    pub output_path: Option<PathBuf>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Contract {
    #[serde(rename(deserialize = "type"))]
    pub c_type: Option<ContractType>,
    pub lang: Option<SupportedContractLangs>,
    pub output_dir: Option<PathBuf>,
}

#[derive(Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ContractType {
    Standard,
    WebApp,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum SupportedContractLangs {
    Rust,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct WebAppContract {
    pub lang: SupportedWebLangs,
    pub typescript: Option<TypescriptConfig>,
    #[serde(rename = "state-sources")]
    pub state_sources: Option<Sources>,
    pub metadata: Option<PathBuf>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum SupportedWebLangs {
    Javascript,
    Typescript,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct TypescriptConfig {
    #[serde(default)]
    pub webpack: bool,
}

fn build_web_state(config: &BuildToolConfig) -> Result<(), DynError> {
    let metadata = if let Some(md) = config.webapp.as_ref().and_then(|a| a.metadata.as_ref()) {
        let mut buf = vec![];
        File::open(md)?.read_to_end(&mut buf)?;
        buf
    } else {
        vec![]
    };

    let mut archive: Builder<Cursor<Vec<u8>>> = Builder::new(Cursor::new(Vec::new()));
    if let Some(web_config) = &config.webapp {
        println!("Bundling webapp contract state");
        match &web_config.lang {
            SupportedWebLangs::Typescript => {
                let webpack = web_config
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
            }
            SupportedWebLangs::Javascript => todo!(),
        }
    } else {
        println!("No webapp config found.");
        return Ok(());
    }

    let build_state = |sources: &Sources| -> Result<(), DynError> {
        let mut found_entry = false;
        if let Some(sources) = &sources.files {
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
        if let Some(src_dirs) = &sources.source_dirs {
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

        if sources.source_dirs.is_none() && sources.files.is_none() {
            return Err("need to specify source dirs and/or files".into());
        }
        if !found_entry {
            return Err("didn't find entry point `index.html` in package".into());
        } else {
            let state = WebApp::from_data(metadata, archive)?;
            let packed = state.pack()?;
            output_artifact(&sources.output_path, &packed)?;
            println!("Finished bundling webapp contract state");
        }

        Ok(())
    };

    if let Some(sources) = config
        .webapp
        .as_ref()
        .and_then(|a| a.state_sources.as_ref())
    {
        build_state(sources)
    } else {
        todo!()
    }
}

fn build_generic_state(config: &mut BuildToolConfig) -> Result<(), DynError> {
    const REQ_ONE_FILE_ERR: &str = "Requires exactly one source file specified for the state.";

    let sources = config.state.as_mut().and_then(|s| s.files.as_mut());
    let sources = if let Some(s) = sources {
        s
    } else {
        return Ok(());
    };

    let output_path = config
        .contract
        .output_dir
        .clone()
        .map(Ok)
        .unwrap_or_else(|| get_default_ouput_dir().map(|p| p.join(DEFAULT_OUTPUT_NAME)))?;

    println!("Bundling contract state");
    let state: PathBuf = (sources.len() == 1)
        .then(|| sources.pop().unwrap())
        .ok_or_else(|| Error::MissConfiguration(REQ_ONE_FILE_ERR.into()))?
        .into();
    std::fs::copy(state, &output_path)?;
    println!("Finished bundling state");
    Ok(())
}

#[inline]
fn get_default_ouput_dir() -> std::io::Result<PathBuf> {
    let output = env::current_dir()?.join("build").join("locutus");
    fs::create_dir_all(&output)?;
    Ok(output)
}

fn output_artifact(output: &Option<PathBuf>, packed: &[u8]) -> Result<(), DynError> {
    if let Some(path) = output {
        File::create(&path)?.write_all(packed)?;
    } else {
        let default_out_dir = get_default_ouput_dir()?;
        fs::create_dir_all(&default_out_dir)?;
        let mut f = File::create(default_out_dir.join(DEFAULT_OUTPUT_NAME))?;
        f.write_all(packed)?;
    }
    Ok(())
}

fn compile_contract(config: &BuildToolConfig, cwd: &Path) -> Result<(), DynError> {
    let work_dir = match config.contract.c_type.unwrap_or(ContractType::Standard) {
        ContractType::WebApp => cwd.join("container"),
        ContractType::Standard => cwd.to_path_buf(),
    };
    match config.contract.lang {
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
                    Error::CommandFailed("cargo")
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
            let mut out_file = if let Some(output) = &config.contract.output_dir {
                output.join(package_name)
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

    fn setup_webapp_contract() -> Result<(BuildToolConfig, PathBuf), DynError> {
        const CRATE_DIR: &str = env!("CARGO_MANIFEST_DIR");
        let cwd = PathBuf::from(CRATE_DIR).join("../../apps/freenet-microblogging/web");
        env::set_current_dir(&cwd)?;
        Ok((
            BuildToolConfig {
                contract: Contract {
                    c_type: Some(ContractType::WebApp),
                    lang: Some(SupportedContractLangs::Rust),
                    output_dir: None,
                },
                state: None,
                webapp: Some(WebAppContract {
                    lang: SupportedWebLangs::Typescript,
                    typescript: Some(TypescriptConfig { webpack: true }),
                    state_sources: Some(Sources {
                        source_dirs: Some(vec!["dist".into()]),
                        files: None,
                        output_path: None,
                    }),
                    metadata: None,
                }),
            },
            cwd,
        ))
    }

    #[test]
    fn package_webapp_state() -> Result<(), DynError> {
        let (config, _cwd) = setup_webapp_contract()?;
        build_web_state(&config)?;

        let mut buf = vec![];
        File::open(
            PathBuf::from("target")
                .join("locutus")
                .join(DEFAULT_OUTPUT_NAME),
        )?
        .read_to_end(&mut buf)?;
        let state = locutus_runtime::locutus_stdlib::interface::State::from(buf);
        let mut web = WebApp::try_from(state.as_ref()).unwrap();

        let target = env::temp_dir().join("locutus-unpack-state");
        let e = web.unpack(&target);
        let unpacked_successfully = target.join("index.html").exists();

        fs::remove_dir_all(target)?;
        e?;
        assert!(unpacked_successfully, "failed to unpack state");

        Ok(())
    }

    #[test]
    fn compile_webapp_contract() -> Result<(), DynError> {
        let (config, cwd) = setup_webapp_contract()?;
        compile_contract(&config, &cwd)?;
        Ok(())
    }

    #[test]
    fn package_generic_state() -> Result<(), DynError> {
        const CRATE_DIR: &str = env!("CARGO_MANIFEST_DIR");
        let cwd = PathBuf::from(CRATE_DIR).join("../../apps/freenet-microblogging/contracts/posts");
        env::set_current_dir(&cwd)?;
        let mut config = BuildToolConfig {
            contract: Contract {
                c_type: Some(ContractType::Standard),
                lang: Some(SupportedContractLangs::Rust),
                output_dir: None,
            },
            state: Some(Sources {
                source_dirs: None,
                files: Some(vec!["initial_state.json".into()]),
                output_path: None,
            }),
            webapp: None,
        };

        build_generic_state(&mut config)?;

        assert!(cwd
            .join("target")
            .join("locutus")
            .join(DEFAULT_OUTPUT_NAME)
            .exists());

        Ok(())
    }
}

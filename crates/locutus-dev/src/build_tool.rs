use std::{
    collections::HashMap,
    env,
    fs::{self, File},
    io::{Cursor, Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use locutus_runtime::{locutus_stdlib::web::WebApp, ContractCode};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use tar::Builder;

use crate::{config::BuildToolCliConfig, util::pipe_std_streams, DynError, Error};

const DEFAULT_OUTPUT_NAME: &str = "contract-state";

pub fn build_package(_cli_config: BuildToolCliConfig, cwd: &Path) -> Result<(), DynError> {
    let mut config = get_config(cwd)?;
    compile_contract(&config, cwd)?;
    match config.contract.c_type.unwrap_or(ContractType::Standard) {
        ContractType::WebApp => {
            let embedded =
                if let Some(d) = config.webapp.as_ref().and_then(|a| a.dependencies.as_ref()) {
                    let deps = include_deps(d)?;
                    embed_deps(cwd, deps)?
                } else {
                    EmbeddedDeps::default()
                };
            build_web_state(&config, embedded, cwd)?
        }
        ContractType::Standard => build_generic_state(&mut config, cwd)?,
    }
    Ok(())
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
    #[serde(rename = "type")]
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
    pub dependencies: Option<toml::value::Table>,
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

fn build_web_state(
    config: &BuildToolConfig,
    embedded_deps: EmbeddedDeps,
    cwd: &Path,
) -> Result<(), DynError> {
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
                    let child = Command::new("npm")
                        .args(&["install"])
                        .current_dir(cwd)
                        .stdout(Stdio::piped())
                        .stderr(Stdio::piped())
                        .spawn()
                        .map_err(|e| {
                            eprintln!("Error while installing npm packages: {e}");
                            Error::CommandFailed("npm")
                        })?;
                    pipe_std_streams(child)?;
                    let cmd_args: &[&str] =
                        if atty::is(atty::Stream::Stdout) && atty::is(atty::Stream::Stderr) {
                            &["--color"]
                        } else {
                            &[]
                        };
                    let child = Command::new("webpack")
                        .args(cmd_args)
                        .current_dir(cwd)
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
                        .current_dir(cwd)
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
                    archive.append_file(cwd.join(p), &mut f)?;
                }
            }
        }
        if let Some(src_dirs) = &sources.source_dirs {
            for dir in src_dirs {
                let ori_dir = cwd.join(dir);
                if ori_dir.is_dir() {
                    let present_entry = ori_dir.join("index.html").exists();
                    if !found_entry && present_entry {
                        found_entry = true;
                    } else if present_entry {
                        return Err(format!(
                            "duplicate entry point (index.html) found at directory: {dir:?}"
                        )
                        .into());
                    }
                    archive.append_dir_all(".", &ori_dir)?;
                } else {
                    return Err(format!("unknown directory: {dir:?}").into());
                }
            }
        }

        if !embedded_deps.code.is_empty() {
            for (hash, code) in embedded_deps.code {
                let mut header = tar::Header::new_gnu();
                header.set_size(code.data().len() as u64);
                header.set_cksum();
                archive.append_data(&mut header, format!("contracts/{hash}.wasm"), code.data())?;
            }
            let mut header = tar::Header::new_gnu();
            header.set_size(embedded_deps.dependencies.len() as u64);
            header.set_cksum();
            let serialized_deps = serde_json::to_vec(&embedded_deps.dependencies)?;
            archive.append_data(
                &mut header,
                "contracts/dependencies.json",
                serialized_deps.as_slice(),
            )?;
        }

        if sources.source_dirs.is_none() && sources.files.is_none() {
            return Err("need to specify source dirs and/or files".into());
        }
        if !found_entry {
            return Err("didn't find entry point `index.html` in package".into());
        } else {
            let state = WebApp::from_data(metadata, archive)?;
            let packed = state.pack()?;
            output_artifact(&sources.output_path, &packed, cwd)?;
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

fn build_generic_state(config: &mut BuildToolConfig, cwd: &Path) -> Result<(), DynError> {
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
        .unwrap_or_else(|| get_default_ouput_dir(cwd).map(|p| p.join(DEFAULT_OUTPUT_NAME)))?;

    println!("Bundling contract state");
    let state: PathBuf = (sources.len() == 1)
        .then(|| sources.pop().unwrap())
        .ok_or_else(|| Error::MissConfiguration(REQ_ONE_FILE_ERR.into()))?
        .into();
    std::fs::copy(cwd.join(state), &output_path)?;
    println!("Finished bundling state");
    Ok(())
}

#[inline]
fn get_default_ouput_dir(cwd: &Path) -> std::io::Result<PathBuf> {
    let output = cwd.join("build").join("locutus");
    fs::create_dir_all(&output)?;
    Ok(output)
}

fn output_artifact(output: &Option<PathBuf>, packed: &[u8], cwd: &Path) -> Result<(), DynError> {
    if let Some(path) = output {
        File::create(&path)?.write_all(packed)?;
    } else {
        let default_out_dir = get_default_ouput_dir(cwd)?;
        fs::create_dir_all(&default_out_dir)?;
        let mut f = File::create(default_out_dir.join(DEFAULT_OUTPUT_NAME))?;
        f.write_all(packed)?;
    }
    Ok(())
}

fn get_config(cwd: &Path) -> Result<BuildToolConfig, DynError> {
    let config_file = cwd.join("locutus.toml");
    if config_file.exists() {
        let mut f_content = vec![];
        File::open(config_file)?.read_to_end(&mut f_content)?;
        Ok(toml::from_slice(&f_content)?)
    } else {
        Err("could not locate `locutus.toml` config file in current dir".into())
    }
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

            let (package_name, output_lib) = get_out_lib(&work_dir)?;
            if !output_lib.exists() {
                return Err(Error::MissConfiguration(
                    format!("couldn't find output file: {output_lib:?}").into(),
                )
                .into());
            }
            let mut out_file = if let Some(output) = &config.contract.output_dir {
                output.join(package_name)
            } else {
                get_default_ouput_dir(cwd)?.join(package_name)
            };
            out_file.set_extension("wasm");
            fs::copy(output_lib, out_file)?;
        }
        None => println!("no lang specified, skipping contract compilation"),
    }
    println!("Contract compiled");
    Ok(())
}

fn get_out_lib(work_dir: &Path) -> Result<(String, PathBuf), DynError> {
    const ERR: &str = "Cargo.toml definition incorrect";
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
    Ok((package_name, output_lib))
}

#[skip_serializing_none]
#[derive(Default, Serialize)]
struct DependencyDefinition {
    path: Option<String>,
    wasm: Option<String>,
}

fn include_deps(
    contracts: &toml::value::Table,
) -> Result<HashMap<&String, DependencyDefinition>, DynError> {
    let mut deps = HashMap::with_capacity(contracts.len());
    for (alias, definition) in contracts {
        let mut dep = DependencyDefinition::default();
        match definition {
            toml::Value::Table(table) => {
                for (k, v) in table {
                    match (k.as_str(), v) {
                        ("path", toml::Value::String(path)) => {
                            if table.contains_key("key") {
                                return Err(Error::MissConfiguration(
                                    "key `path` is mutually exclusive with `key`".into(),
                                )
                                .into());
                            }
                            dep.path = Some(path.clone());
                        }
                        (k, _) => {
                            return Err(Error::MissConfiguration(
                                format!("unknown key: {k}").into(),
                            )
                            .into());
                        }
                    }
                }
            }
            _ => panic!(),
        }
        deps.insert(alias, dep);
    }
    Ok(deps)
}

type CodeHash = String;

#[derive(Default)]
struct EmbeddedDeps {
    code: HashMap<CodeHash, ContractCode<'static>>,
    dependencies: HashMap<String, DependencyDefinition>,
}

fn embed_deps(
    cwd: &Path,
    deps: HashMap<impl Into<String>, DependencyDefinition>,
) -> Result<EmbeddedDeps, DynError> {
    let cwd = fs::canonicalize(cwd)?;
    let mut deps_json = HashMap::new();
    let mut to_embed = EmbeddedDeps::default();
    for (alias, dep) in deps.into_iter() {
        if let Some(path) = &dep.path {
            let path = cwd.join(path);
            let config = get_config(&path)?;
            compile_contract(&config, &path)?;
            let mut buf = vec![];
            let (_pname, out) = get_out_lib(&path)?;
            let mut f = File::open(out)?;
            f.read_to_end(&mut buf)?;
            let code = ContractCode::from(buf);
            let code_hash = code.hash_str();
            to_embed.code.insert(code_hash.clone(), code);
            deps_json.insert(
                alias.into(),
                DependencyDefinition {
                    wasm: Some(code_hash),
                    ..Default::default()
                },
            );
        }
    }
    to_embed.dependencies = deps_json;
    Ok(to_embed)
}

#[cfg(test)]
mod test {
    use super::*;

    fn setup_webapp_contract() -> Result<(BuildToolConfig, PathBuf), DynError> {
        const CRATE_DIR: &str = env!("CARGO_MANIFEST_DIR");
        let cwd = PathBuf::from(CRATE_DIR).join("../../apps/freenet-microblogging/web");
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
                    dependencies: Some(
                        toml::toml! {
                            posts = { path = "../contracts/posts" }
                        }
                        .as_table()
                        .unwrap()
                        .clone(),
                    ),
                }),
            },
            cwd,
        ))
    }

    #[test]
    fn package_webapp_state() -> Result<(), DynError> {
        let (config, cwd) = setup_webapp_contract()?;
        // env::set_current_dir(&cwd)?;
        build_web_state(&config, EmbeddedDeps::default(), &cwd)?;

        let mut buf = vec![];
        File::open(cwd.join("build").join("locutus").join(DEFAULT_OUTPUT_NAME))?
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

        build_generic_state(&mut config, &cwd)?;

        assert!(cwd
            .join("build")
            .join("locutus")
            .join(DEFAULT_OUTPUT_NAME)
            .exists());

        Ok(())
    }

    #[test]
    fn deps_parsing() -> Result<(), DynError> {
        let deps = toml::toml! {
            posts = { path = "../contracts/posts" }
        };
        println!("{:?}", deps.as_table().unwrap().clone());
        include_deps(deps.as_table().unwrap())?;
        Ok(())
    }

    #[test]
    fn embedded_deps() -> Result<(), DynError> {
        const CRATE_DIR: &str = env!("CARGO_MANIFEST_DIR");
        let cwd = PathBuf::from(CRATE_DIR).join("../../apps/freenet-microblogging/web");
        let deps = toml::toml! {
            posts = { path = "../contracts/posts" }
        };
        let defs = include_deps(deps.as_table().unwrap())?;
        embed_deps(&cwd, defs)?;
        Ok(())
    }
}

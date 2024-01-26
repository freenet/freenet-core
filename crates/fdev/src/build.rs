use freenet::server::WebApp;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::{
    collections::HashMap,
    env,
    fs::{self, File},
    io::{Cursor, Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
};
use tar::Builder;

use crate::{
    config::{BuildToolConfig, PackageType},
    util::pipe_std_streams,
    Error,
};
pub(crate) use contract::*;

const DEFAULT_OUTPUT_NAME: &str = "contract-state";
const WASM_TARGET: &str = "wasm32-unknown-unknown";

#[cfg(windows)]
pub const NPM_BUILD_COMMAND: &'static str = "npm.cmd";
#[cfg(windows)]
pub const TSC_BUILD_COMMAND: &'static str = "tsc.cmd";
#[cfg(windows)]
pub const WEBPACK_BUILD_COMMAND: &'static str = "webpack.cmd";

#[cfg(not(windows))]
pub const NPM_BUILD_COMMAND: &str = "npm";
#[cfg(not(windows))]
pub const TSC_BUILD_COMMAND: &str = "tsc";
#[cfg(not(windows))]
pub const WEBPACK_BUILD_COMMAND: &str = "webpack";

pub fn build_package(cli_config: BuildToolConfig, cwd: &Path) -> Result<(), anyhow::Error> {
    match cli_config.package_type {
        PackageType::Contract => contract::package_contract(cli_config, cwd),
        PackageType::Delegate => delegate::package_delegate(cli_config, cwd),
    }
}

fn compile_options(cli_config: &BuildToolConfig) -> impl Iterator<Item = String> {
    let release: &[&str] = if cli_config.debug {
        &[]
    } else {
        &["--release"]
    };
    let feature_list = cli_config
        .features
        .iter()
        .flat_map(|s| {
            s.split(',')
                .filter(|p| *p != cli_config.package_type.feature())
        })
        .chain([cli_config.package_type.feature()]);
    let features = [
        "--features".to_string(),
        feature_list.collect::<Vec<_>>().join(","),
    ];
    features
        .into_iter()
        .chain(release.iter().map(|s| s.to_string()))
}

#[cfg(test)]
#[test]
fn test_get_compile_options() {
    let config = BuildToolConfig {
        features: Some("contract".into()),
        version: semver::Version::new(0, 0, 1),
        package_type: PackageType::Contract,
        debug: false,
    };
    let opts: Vec<_> = compile_options(&config).collect();
    assert_eq!(
        opts,
        vec!["--features", "contract,freenet-main-contract", "--release"]
    );
}

fn compile_rust_wasm_lib(
    cli_config: &BuildToolConfig,
    work_dir: &Path,
) -> Result<(), anyhow::Error> {
    const RUST_TARGET_ARGS: &[&str] = &["build", "--lib", "--target"];
    use std::io::IsTerminal;
    let comp_opts = compile_options(cli_config).collect::<Vec<_>>();
    let cmd_args = if std::io::stdout().is_terminal() && std::io::stderr().is_terminal() {
        RUST_TARGET_ARGS
            .iter()
            .copied()
            .chain([WASM_TARGET, "--color", "always"])
            .chain(comp_opts.iter().map(|s| s.as_str()))
            .collect::<Vec<_>>()
    } else {
        RUST_TARGET_ARGS
            .iter()
            .copied()
            .chain([WASM_TARGET])
            .chain(comp_opts.iter().map(|s| s.as_str()))
            .collect::<Vec<_>>()
    };

    let package_type = cli_config.package_type;
    println!("Compiling {package_type} with rust");
    let child = Command::new("cargo")
        .args(&cmd_args)
        .current_dir(work_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| {
            eprintln!("Error while executing cargo command: {e}");
            Error::CommandFailed("cargo")
        })?;
    pipe_std_streams(child)?;
    Ok(())
}

fn get_out_lib(
    work_dir: &Path,
    cli_config: &BuildToolConfig,
) -> Result<(String, PathBuf), anyhow::Error> {
    const ERR: &str = "Cargo.toml definition incorrect";

    let target = WASM_TARGET;

    let mut f_content = vec![];
    File::open(work_dir.join("Cargo.toml"))?.read_to_end(&mut f_content)?;
    let cargo_config: toml::Value = toml::from_str(std::str::from_utf8(&f_content)?)?;
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
    let opt_dir = if !cli_config.debug {
        "release"
    } else {
        "debug"
    };
    let output_lib = env::var("CARGO_TARGET_DIR")
        .map_err(|e| {
            println!("Missing environment variable `CARGO_TARGET_DIR");
            e
        })?
        .parse::<PathBuf>()?
        .join(target)
        .join(opt_dir)
        .join(&package_name)
        .with_extension("wasm");
    Ok((package_name, output_lib))
}

fn get_default_ouput_dir(cwd: &Path) -> std::io::Result<PathBuf> {
    let output = cwd.join("build").join("freenet");
    fs::create_dir_all(&output)?;
    Ok(output)
}

mod contract {
    use freenet_stdlib::prelude::ContractCode;

    use super::*;

    pub(super) fn package_contract(
        cli_config: BuildToolConfig,
        cwd: &Path,
    ) -> Result<(), anyhow::Error> {
        let mut config = get_config(cwd)?;
        compile_contract(&config, &cli_config, cwd)?;
        match config.contract.c_type.unwrap_or(ContractType::Standard) {
            ContractType::WebApp => {
                println!("Packaging standard Freenet web app contract type");
                let embedded =
                    if let Some(d) = config.webapp.as_ref().and_then(|a| a.dependencies.as_ref()) {
                        let deps = include_deps(d)?;
                        embed_deps(cwd, deps, &cli_config)?
                    } else {
                        EmbeddedDeps::default()
                    };
                build_web_state(&config, embedded, cwd)?
            }
            ContractType::Standard => {
                println!("Packaging generic contract type");
                build_generic_state(&mut config, cwd)?
            }
        }
        Ok(())
    }

    #[derive(Serialize, Deserialize)]
    pub(crate) struct ContractBuildConfig {
        pub contract: Contract,
        pub state: Option<Sources>,
        pub webapp: Option<WebAppContract>,
    }

    #[derive(Serialize, Deserialize)]
    pub(crate) struct Sources {
        pub source_dirs: Option<Vec<PathBuf>>,
        pub files: Option<Vec<String>>,
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
        pub lang: Option<SupportedWebLangs>,
        pub typescript: Option<TypescriptConfig>,
        #[serde(rename = "state-sources")]
        pub state_sources: Sources,
        pub metadata: Option<PathBuf>,
        pub dependencies: Option<toml::value::Table>,
    }

    #[derive(Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "lowercase")]
    pub(crate) enum SupportedWebLangs {
        Typescript,
    }

    #[derive(Serialize, Deserialize)]
    pub(crate) struct TypescriptConfig {
        #[serde(default)]
        pub webpack: bool,
    }

    fn build_web_state(
        config: &ContractBuildConfig,
        embedded_deps: EmbeddedDeps,
        cwd: &Path,
    ) -> Result<(), anyhow::Error> {
        let Some(web_config) = &config.webapp else {
            println!("No webapp config found.");
            return Ok(());
        };

        let metadata = if let Some(md) = config.webapp.as_ref().and_then(|a| a.metadata.as_ref()) {
            let mut buf = vec![];
            File::open(md)?.read_to_end(&mut buf)?;
            buf
        } else {
            vec![]
        };

        let mut archive: Builder<Cursor<Vec<u8>>> = Builder::new(Cursor::new(Vec::new()));
        println!("Bundling webapp contract state");
        match &web_config.lang {
            Some(SupportedWebLangs::Typescript) => {
                let child = Command::new(NPM_BUILD_COMMAND)
                    .args(["install"])
                    .current_dir(cwd)
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .map_err(|e| {
                        eprintln!("Error while installing npm packages: {e}");
                        Error::CommandFailed(NPM_BUILD_COMMAND)
                    })?;
                pipe_std_streams(child)?;
                let webpack = web_config
                    .typescript
                    .as_ref()
                    .map(|c| c.webpack)
                    .unwrap_or_default();
                use std::io::IsTerminal;
                if webpack {
                    let cmd_args: &[&str] = if std::io::stdout().is_terminal()
                        && std::io::stderr().is_terminal()
                        && cfg!(not(windows))
                    {
                        &["--color"]
                    } else {
                        &[]
                    };
                    let child = Command::new(WEBPACK_BUILD_COMMAND)
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
                        if std::io::stdout().is_terminal() && std::io::stderr().is_terminal() {
                            &["--pretty"]
                        } else {
                            &[]
                        };
                    let child = Command::new(TSC_BUILD_COMMAND)
                        .args(cmd_args)
                        .current_dir(cwd)
                        .stdout(Stdio::piped())
                        .stderr(Stdio::piped())
                        .spawn()
                        .map_err(|e| {
                            eprintln!("Error while executing command tsc: {e}");
                            Error::CommandFailed(TSC_BUILD_COMMAND)
                        })?;
                    pipe_std_streams(child)?;
                    println!("Compiled input using tsc");
                }
            }
            None => {}
        }

        let build_state = |sources: &Sources| -> Result<(), anyhow::Error> {
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
                            anyhow::bail!(
                                "duplicate entry point (index.html) found at directory: {dir:?}"
                            );
                        }
                        archive.append_dir_all(".", &ori_dir)?;
                    } else {
                        anyhow::bail!("unknown directory: {dir:?}");
                    }
                }
            }

            if !embedded_deps.code.is_empty() {
                for (hash, code) in embedded_deps.code {
                    let mut header = tar::Header::new_gnu();
                    header.set_size(code.data().len() as u64);
                    header.set_cksum();
                    archive.append_data(
                        &mut header,
                        format!("contracts/{hash}.wasm"),
                        code.data(),
                    )?;
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
                anyhow::bail!("need to specify source dirs and/or files");
            }
            if !found_entry {
                anyhow::bail!("didn't find entry point `index.html` in package");
            } else {
                let state = WebApp::from_data(metadata, archive)?;
                let packed = state.pack()?;
                output_artifact(&config.contract.output_dir, &packed, cwd)?;
                println!("Finished bundling webapp contract state");
            }

            Ok(())
        };

        let sources = &web_config.state_sources;
        build_state(sources)
    }

    fn build_generic_state(
        config: &mut ContractBuildConfig,
        cwd: &Path,
    ) -> Result<(), anyhow::Error> {
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
        std::fs::copy(cwd.join(state), output_path)?;
        println!("Finished bundling state");
        Ok(())
    }

    fn output_artifact(
        output: &Option<PathBuf>,
        packed: &[u8],
        cwd: &Path,
    ) -> Result<(), anyhow::Error> {
        if let Some(path) = output {
            File::create(path)?.write_all(packed)?;
        } else {
            let default_out_dir = get_default_ouput_dir(cwd)?;
            fs::create_dir_all(&default_out_dir)?;
            let mut f = File::create(default_out_dir.join(DEFAULT_OUTPUT_NAME))?;
            f.write_all(packed)?;
        }
        Ok(())
    }

    fn get_config(cwd: &Path) -> Result<ContractBuildConfig, anyhow::Error> {
        let config_file = cwd.join("freenet.toml");
        if config_file.exists() {
            let mut f_content = vec![];
            File::open(config_file)?.read_to_end(&mut f_content)?;
            Ok(toml::from_str(std::str::from_utf8(&f_content)?)?)
        } else {
            anyhow::bail!("could not locate `freenet.toml` config file in current dir")
        }
    }

    fn compile_contract(
        config: &ContractBuildConfig,
        cli_config: &BuildToolConfig,
        cwd: &Path,
    ) -> Result<(), anyhow::Error> {
        let work_dir = match config.contract.c_type.unwrap_or(ContractType::Standard) {
            ContractType::WebApp => cwd.join("container"),
            ContractType::Standard => cwd.to_path_buf(),
        };
        match config.contract.lang {
            Some(SupportedContractLangs::Rust) => {
                compile_rust_wasm_lib(cli_config, &work_dir)?;
                let (package_name, output_lib) = get_out_lib(&work_dir, cli_config)?;
                if !output_lib.exists() {
                    return Err(Error::MissConfiguration(
                        format!("couldn't find output file: {output_lib:?}").into(),
                    )
                    .into());
                }
                let out_file = if let Some(output) = &config.contract.output_dir {
                    output.join(package_name)
                } else {
                    get_default_ouput_dir(cwd)?.join(package_name)
                };
                let output = get_versioned_contract(&output_lib, cli_config)?;
                let mut file = File::create(out_file)?;
                file.write_all(output.as_slice())?;
            }
            None => println!("no lang specified, skipping contract compilation"),
        }
        println!("Contract compiled");
        Ok(())
    }

    fn get_versioned_contract(
        contract_code_path: &Path,
        cli_config: &BuildToolConfig,
    ) -> Result<Vec<u8>, anyhow::Error> {
        let code: ContractCode = ContractCode::load_raw(contract_code_path)?;
        tracing::info!("compiled contract code hash: {}", code.hash_str());
        let output = code
            .to_bytes_versioned(
                (&cli_config.version)
                    .try_into()
                    .map_err(anyhow::Error::msg)?,
            )
            .map_err(anyhow::Error::msg)?;
        Ok(output)
    }

    #[skip_serializing_none]
    #[derive(Default, Serialize)]
    struct DependencyDefinition {
        path: Option<String>,
        wasm: Option<String>,
    }

    fn include_deps(
        contracts: &toml::value::Table,
    ) -> Result<HashMap<&String, DependencyDefinition>, anyhow::Error> {
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
        cli_config: &BuildToolConfig,
    ) -> Result<EmbeddedDeps, anyhow::Error> {
        let cwd = fs::canonicalize(cwd)?;
        let mut deps_json = HashMap::new();
        let mut to_embed = EmbeddedDeps::default();
        for (alias, dep) in deps.into_iter() {
            if let Some(path) = &dep.path {
                let path = cwd.join(path);
                let config = get_config(&path)?;
                compile_contract(&config, cli_config, &path)?;
                let mut buf = vec![];
                let (_pname, out) = get_out_lib(&path, cli_config)?;
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

        fn setup_webapp_contract() -> Result<(ContractBuildConfig, PathBuf), anyhow::Error> {
            const CRATE_DIR: &str = env!("CARGO_MANIFEST_DIR");
            let cwd = PathBuf::from(CRATE_DIR).join("../../tests/test-app-1");
            Ok((
                ContractBuildConfig {
                    contract: Contract {
                        c_type: Some(ContractType::WebApp),
                        lang: Some(SupportedContractLangs::Rust),
                        output_dir: None,
                    },
                    state: None,
                    webapp: Some(WebAppContract {
                        lang: Some(SupportedWebLangs::Typescript),
                        typescript: Some(TypescriptConfig { webpack: true }),
                        state_sources: Sources {
                            source_dirs: Some(vec!["dist".into()]),
                            files: None,
                        },
                        metadata: None,
                        dependencies: Some(
                            toml::toml! {
                                posts = { path = "deps" }
                            }
                            .clone(),
                        ),
                    }),
                },
                cwd,
            ))
        }

        // FIXME: This test fails in GitHub CI. The failure is due to issues compiling the test-app-1 application with webpack.
        #[test]
        #[ignore]
        fn package_webapp_state() -> Result<(), anyhow::Error> {
            let (config, cwd) = setup_webapp_contract()?;
            // env::set_current_dir(&cwd)?;
            build_web_state(&config, EmbeddedDeps::default(), &cwd)?;

            let mut buf = vec![];
            File::open(cwd.join("build").join("freenet").join(DEFAULT_OUTPUT_NAME))?
                .read_to_end(&mut buf)?;
            let state = freenet_stdlib::prelude::State::from(buf);
            let mut web = WebApp::try_from(state.as_ref()).unwrap();

            let target = env::temp_dir().join("freenet-unpack-state");
            let e = web.unpack(&target);
            let unpacked_successfully = target.join("index.html").exists();

            fs::remove_dir_all(target)?;
            e?;
            assert!(unpacked_successfully, "failed to unpack state");

            Ok(())
        }

        #[test]
        fn compile_webapp_contract() -> Result<(), anyhow::Error> {
            //
            let (config, cwd) = setup_webapp_contract()?;
            compile_contract(&config, &BuildToolConfig::default(), &cwd)?;
            Ok(())
        }

        #[test]
        fn package_generic_state() -> Result<(), anyhow::Error> {
            const CRATE_DIR: &str = env!("CARGO_MANIFEST_DIR");
            let cwd = PathBuf::from(CRATE_DIR).join("../../tests/test-app-1/deps");
            let mut config = ContractBuildConfig {
                contract: Contract {
                    c_type: Some(ContractType::Standard),
                    lang: Some(SupportedContractLangs::Rust),
                    output_dir: None,
                },
                state: Some(Sources {
                    source_dirs: None,
                    files: Some(vec!["initial_state.json".into()]),
                }),
                webapp: None,
            };

            build_generic_state(&mut config, &cwd)?;

            assert!(cwd
                .join("build")
                .join("freenet")
                .join(DEFAULT_OUTPUT_NAME)
                .exists());

            Ok(())
        }

        #[test]
        fn deps_parsing() -> Result<(), anyhow::Error> {
            let deps = toml::toml! {
                posts = { path = "deps" }
            };
            println!("{:?}", deps.clone());
            include_deps(&deps)?;
            Ok(())
        }

        #[test]
        fn embedded_deps() -> Result<(), anyhow::Error> {
            const CRATE_DIR: &str = env!("CARGO_MANIFEST_DIR");
            let cwd = PathBuf::from(CRATE_DIR).join("../../tests/test-app-1");
            let deps = toml::toml! {
                posts = { path = "deps" }
            };
            let defs = include_deps(&deps).unwrap();
            embed_deps(&cwd, defs, &BuildToolConfig::default()).unwrap();
            Ok(())
        }
    }
}

mod delegate {
    use freenet_stdlib::prelude::DelegateCode;

    use super::*;

    pub(super) fn package_delegate(
        cli_config: BuildToolConfig,
        cwd: &Path,
    ) -> Result<(), anyhow::Error> {
        compile_rust_wasm_lib(&cli_config, cwd)?;
        let (package_name, output_lib) = get_out_lib(cwd, &cli_config)?;
        if !output_lib.exists() {
            return Err(Error::MissConfiguration(
                format!("couldn't find output file: {output_lib:?}").into(),
            )
            .into());
        }
        let out_file = get_default_ouput_dir(cwd)?.join(package_name);
        let output = get_versioned_contract(&output_lib, &cli_config)?;
        let mut file = File::create(out_file)?;
        file.write_all(output.as_slice())?;
        Ok(())
    }

    fn get_versioned_contract(
        contract_code_path: &Path,
        cli_config: &BuildToolConfig,
    ) -> Result<Vec<u8>, anyhow::Error> {
        let code: DelegateCode = DelegateCode::load_raw(contract_code_path)?;
        tracing::info!("compiled contract code hash: {}", code.hash_str());
        let output = code
            .to_bytes_versioned(
                (&cli_config.version)
                    .try_into()
                    .map_err(anyhow::Error::msg)?,
            )
            .map_err(anyhow::Error::msg)?;
        Ok(output)
    }
}

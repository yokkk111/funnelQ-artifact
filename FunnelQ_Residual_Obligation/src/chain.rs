                                                                               
   
                                                                            
                                                                           

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

const RPC_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_HTTP_RESPONSE_BYTES: u64 = 8 * 1024 * 1024;
                                           
const GET_EPOCH_ROOT_SELECTOR: &str = "a00a4604";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChainPreflightConfig {
    pub rpc_url: String,
    pub registry_address: String,
    pub block_tag: String,
    pub expected_chain_id: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ChainReadEvidence {
    pub mode: String,
    pub rpc_url: String,
    pub chain_id: u64,
    pub expected_chain_id: Option<u64>,
    pub registry_address: String,
    pub epoch_id: String,
    pub read_signature: String,
    pub requested_block_tag: String,
    pub block_number: u64,
    pub block_hash: String,
    pub contract_code_bytes: usize,
    pub epoch_root: String,
    pub chain_context_seconds: f64,
    pub eth_call_seconds: f64,
    pub chain_read_seconds: f64,
}

                                                                             
                                                                             
                       
pub fn optional_chain_preflight_config(
    rpc_url: Option<String>,
    registry_address: Option<String>,
    block_tag: Option<String>,
    expected_chain_id: Option<u64>,
) -> Result<Option<ChainPreflightConfig>> {
    let core_count = [
        rpc_url.is_some(),
        registry_address.is_some(),
        block_tag.is_some(),
    ]
    .into_iter()
    .filter(|present| *present)
    .count();
    if core_count == 0 {
        if expected_chain_id.is_some() {
            bail!("--expected-chain-id requires all three chain preflight options")
        }
        return Ok(None);
    }
    if core_count != 3 {
        bail!(
            "--chain-rpc-url, --registry-address, and --chain-block-tag must be supplied together"
        )
    }
    Ok(Some(ChainPreflightConfig {
        rpc_url: rpc_url.expect("count checked"),
        registry_address: registry_address.expect("count checked"),
        block_tag: block_tag.expect("count checked"),
        expected_chain_id,
    }))
}

                                                                            
                                                   
pub fn read_bound_residual_epoch_root(
    config: &ChainPreflightConfig,
    source_epoch_id: &str,
    expected_epoch_root: &str,
) -> Result<ChainReadEvidence> {
    let total_started = Instant::now();
    let registry_address = normalize_hex_exact(&config.registry_address, 20, "registry address")?;
    let epoch_id = normalize_hex_exact(source_epoch_id, 32, "source epoch id")?;
    let expected_epoch_root = normalize_hex_exact(expected_epoch_root, 32, "Residual epoch root")?;
    let requested_block_tag = normalize_fixed_block_tag(&config.block_tag)?;
    let requested_block_number = parse_hex_quantity(&requested_block_tag, "fixed block tag")?;
    let endpoint = HttpEndpoint::parse(&config.rpc_url)?;
    let mut rpc = HttpJsonRpcClient::new(endpoint);

    let context_started = Instant::now();
    let chain_id = parse_hex_quantity(
        rpc.call("eth_chainId", json!([]))?
            .as_str()
            .context("eth_chainId did not return a string")?,
        "chain id",
    )?;
    if let Some(expected) = config.expected_chain_id {
        if chain_id != expected {
            bail!("RPC chain id {chain_id} differs from expected chain id {expected}")
        }
    }

    let block = rpc.call("eth_getBlockByNumber", json!([requested_block_tag, false]))?;
    let block = block
        .as_object()
        .context("RPC did not return the fixed block")?;
    let block_number = parse_hex_quantity(
        block
            .get("number")
            .and_then(Value::as_str)
            .context("fixed block has no number")?,
        "fixed block number",
    )?;
    if block_number != requested_block_number {
        bail!(
            "RPC returned block {block_number} for requested fixed block {requested_block_number}"
        )
    }
    let block_hash = normalize_hex_exact(
        block
            .get("hash")
            .and_then(Value::as_str)
            .context("fixed block has no hash")?,
        32,
        "fixed block hash",
    )?;

    let code = rpc
        .call(
            "eth_getCode",
            json!([registry_address, requested_block_tag]),
        )?
        .as_str()
        .context("eth_getCode did not return a string")?
        .to_owned();
    let contract_code_bytes = validate_contract_code(&code)?;
    let chain_context_seconds = context_started.elapsed().as_secs_f64();

    let call_data = format!("0x{GET_EPOCH_ROOT_SELECTOR}{}", &epoch_id[2..]);
    let eth_call_started = Instant::now();
    let raw_root = rpc
        .call(
            "eth_call",
            json!([{"to": registry_address, "data": call_data}, requested_block_tag]),
        )?
        .as_str()
        .context("eth_call did not return ABI-encoded bytes32")?
        .to_owned();
    let eth_call_seconds = eth_call_started.elapsed().as_secs_f64();
    let epoch_root = normalize_hex_exact(&raw_root, 32, "on-chain Residual epoch root")?;
    if epoch_root != expected_epoch_root {
        bail!(
            "on-chain epoch root {epoch_root} differs from verified Residual Manifest root {expected_epoch_root}"
        )
    }

    Ok(ChainReadEvidence {
        mode: "ethereum_json_rpc_eth_call".to_owned(),
        rpc_url: rpc.endpoint.display_url.clone(),
        chain_id,
        expected_chain_id: config.expected_chain_id,
        registry_address,
        epoch_id,
        read_signature: "getEpochRoot(bytes32)".to_owned(),
        requested_block_tag,
        block_number,
        block_hash,
        contract_code_bytes,
        epoch_root,
        chain_context_seconds,
        eth_call_seconds,
        chain_read_seconds: total_started.elapsed().as_secs_f64(),
    })
}

fn normalize_hex_exact(value: &str, bytes: usize, label: &str) -> Result<String> {
    let raw = value.trim().strip_prefix("0x").unwrap_or(value.trim());
    if raw.len() != bytes * 2 || !raw.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("{label} must contain exactly {bytes} hexadecimal bytes")
    }
    Ok(format!("0x{}", raw.to_ascii_lowercase()))
}

fn parse_hex_quantity(value: &str, label: &str) -> Result<u64> {
    let raw = value
        .strip_prefix("0x")
        .with_context(|| format!("{label} is not an Ethereum hexadecimal quantity"))?;
    if raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("{label} is not an Ethereum hexadecimal quantity")
    }
    u64::from_str_radix(raw, 16).with_context(|| format!("{label} exceeds u64"))
}

fn normalize_fixed_block_tag(value: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty() {
        bail!("--chain-block-tag must name a fixed numeric block")
    }
    let number = if let Some(raw) = value.strip_prefix("0x") {
        if raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            bail!("--chain-block-tag must name a fixed numeric block")
        }
        u64::from_str_radix(raw, 16).context("--chain-block-tag exceeds u64")?
    } else {
        value
            .parse::<u64>()
            .context("--chain-block-tag must name a fixed numeric block")?
    };
    Ok(format!("0x{number:x}"))
}

fn validate_contract_code(value: &str) -> Result<usize> {
    let raw = value.trim().strip_prefix("0x").unwrap_or(value.trim());
    if raw.is_empty()
        || !raw.len().is_multiple_of(2)
        || !raw.bytes().all(|byte| byte.is_ascii_hexdigit())
        || raw.bytes().all(|byte| byte == b'0')
    {
        bail!("no valid contract bytecode exists at the registry address in the fixed block")
    }
    Ok(raw.len() / 2)
}

#[derive(Clone, Debug)]
struct HttpEndpoint {
    connect_host: String,
    port: u16,
    host_header: String,
    path: String,
    display_url: String,
}

impl HttpEndpoint {
    fn parse(url: &str) -> Result<Self> {
        let value = url.trim();
        let remainder = value
            .strip_prefix("http://")
            .context("chain RPC URL must use http:// (TLS is intentionally not implemented)")?;
        if remainder.is_empty() || remainder.contains('#') || remainder.contains('@') {
            bail!("chain RPC URL is invalid or contains unsupported credentials/fragment")
        }
        let separator = remainder
            .char_indices()
            .find(|(_, character)| matches!(character, '/' | '?'))
            .map(|(index, _)| index);
        let (authority, path) = match separator {
            Some(index) if remainder.as_bytes()[index] == b'?' => {
                (&remainder[..index], format!("/{}", &remainder[index..]))
            }
            Some(index) => (&remainder[..index], remainder[index..].to_owned()),
            None => (remainder, "/".to_owned()),
        };
        if authority.is_empty() {
            bail!("chain RPC URL has no host")
        }

        let (connect_host, port) = if let Some(bracketed) = authority.strip_prefix('[') {
            let end = bracketed
                .find(']')
                .context("invalid bracketed IPv6 RPC host")?;
            let host = &bracketed[..end];
            let suffix = &bracketed[end + 1..];
            let port = if suffix.is_empty() {
                80
            } else {
                suffix
                    .strip_prefix(':')
                    .context("invalid IPv6 RPC authority")?
                    .parse()
                    .context("invalid RPC port")?
            };
            (host.to_owned(), port)
        } else if let Some((host, raw_port)) = authority.rsplit_once(':') {
            if host.is_empty() || raw_port.is_empty() {
                bail!("invalid RPC authority")
            }
            (
                host.to_owned(),
                raw_port.parse().context("invalid RPC port")?,
            )
        } else {
            (authority.to_owned(), 80)
        };
        let display_path = match path.split_once('?') {
            Some((plain_path, _)) => format!("{plain_path}?<redacted>"),
            None => path.to_owned(),
        };
        Ok(Self {
            connect_host,
            port,
            host_header: authority.to_owned(),
            path,
            display_url: format!("http://{authority}{display_path}"),
        })
    }
}

struct HttpJsonRpcClient {
    endpoint: HttpEndpoint,
    next_id: u64,
}

impl HttpJsonRpcClient {
    fn new(endpoint: HttpEndpoint) -> Self {
        Self {
            endpoint,
            next_id: 1,
        }
    }

    fn call(&mut self, method: &str, params: Value) -> Result<Value> {
        let id = self.next_id;
        self.next_id += 1;
        let body = serde_json::to_vec(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        }))?;
        let address = (self.endpoint.connect_host.as_str(), self.endpoint.port)
            .to_socket_addrs()
            .with_context(|| format!("resolve RPC host for {method}"))?
            .next()
            .with_context(|| format!("RPC host resolved to no address for {method}"))?;
        let mut stream = TcpStream::connect_timeout(&address, RPC_TIMEOUT)
            .with_context(|| format!("connect to chain RPC for {method}"))?;
        stream.set_read_timeout(Some(RPC_TIMEOUT))?;
        stream.set_write_timeout(Some(RPC_TIMEOUT))?;
        write!(
            stream,
            "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nAccept: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            self.endpoint.path,
            self.endpoint.host_header,
            body.len()
        )?;
        stream.write_all(&body)?;
        stream.flush()?;

        let mut response = Vec::new();
        stream
            .take(MAX_HTTP_RESPONSE_BYTES + 1)
            .read_to_end(&mut response)
            .with_context(|| format!("read chain RPC response for {method}"))?;
        if response.len() as u64 > MAX_HTTP_RESPONSE_BYTES {
            bail!("chain RPC response for {method} exceeds size limit")
        }
        let body = parse_http_response(&response)
            .with_context(|| format!("decode HTTP response for {method}"))?;
        let message: Value = serde_json::from_slice(&body)
            .with_context(|| format!("decode JSON-RPC response for {method}"))?;
        if message.get("id").and_then(Value::as_u64) != Some(id) {
            bail!("JSON-RPC response id mismatch for {method}")
        }
        if let Some(error) = message.get("error") {
            bail!("JSON-RPC {method} error: {error}")
        }
        message
            .get("result")
            .cloned()
            .with_context(|| format!("JSON-RPC response has no result for {method}"))
    }
}

fn parse_http_response(response: &[u8]) -> Result<Vec<u8>> {
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .context("HTTP response has no header terminator")?;
    let headers =
        std::str::from_utf8(&response[..header_end]).context("HTTP headers are not UTF-8")?;
    let mut lines = headers.split("\r\n");
    let status = lines.next().context("HTTP response has no status line")?;
    let status_code = status
        .split_whitespace()
        .nth(1)
        .context("HTTP status line has no status code")?;
    if status_code != "200" {
        bail!("chain RPC returned HTTP status {status_code}")
    }
    let mut content_length = None;
    let mut chunked = false;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("content-length") {
            content_length = Some(
                value
                    .trim()
                    .parse::<usize>()
                    .context("invalid Content-Length")?,
            );
        }
        if name.eq_ignore_ascii_case("transfer-encoding")
            && value
                .split(',')
                .any(|encoding| encoding.trim().eq_ignore_ascii_case("chunked"))
        {
            chunked = true;
        }
    }
    let body = &response[header_end + 4..];
    if chunked {
        return decode_chunked(body);
    }
    if let Some(expected) = content_length {
        if body.len() != expected {
            bail!("HTTP response body length differs from Content-Length")
        }
    }
    Ok(body.to_vec())
}

fn decode_chunked(mut input: &[u8]) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    loop {
        let line_end = input
            .windows(2)
            .position(|window| window == b"\r\n")
            .context("truncated chunk length")?;
        let raw_length = std::str::from_utf8(&input[..line_end])?;
        let raw_length = raw_length.split(';').next().unwrap_or(raw_length).trim();
        let length = usize::from_str_radix(raw_length, 16).context("invalid chunk length")?;
        input = &input[line_end + 2..];
        if length == 0 {
            return Ok(output);
        }
        if input.len() < length + 2 || &input[length..length + 2] != b"\r\n" {
            bail!("truncated HTTP chunk")
        }
        output.extend_from_slice(&input[..length]);
        input = &input[length + 2..];
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::BufRead;
    use std::net::TcpListener;
    use std::thread;

    fn start_mock_rpc(returned_root: String) -> (String, thread::JoinHandle<Result<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            let mut methods = Vec::new();
            for _ in 0..4 {
                let (stream, _) = listener.accept()?;
                stream.set_read_timeout(Some(Duration::from_secs(2)))?;
                let mut reader = std::io::BufReader::new(stream.try_clone()?);
                let mut content_length = None;
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line)?;
                    if line == "\r\n" {
                        break;
                    }
                    if let Some((name, value)) = line.split_once(':') {
                        if name.eq_ignore_ascii_case("content-length") {
                            content_length = Some(value.trim().parse::<usize>()?);
                        }
                    }
                }
                let mut body = vec![0_u8; content_length.context("mock request has no length")?];
                reader.read_exact(&mut body)?;
                let request: Value = serde_json::from_slice(&body)?;
                let id = request.get("id").and_then(Value::as_u64).context("id")?;
                let method = request
                    .get("method")
                    .and_then(Value::as_str)
                    .context("method")?
                    .to_owned();
                let result = match method.as_str() {
                    "eth_chainId" => json!("0x7a69"),
                    "eth_getBlockByNumber" => json!({
                        "number": "0x2",
                        "hash": format!("0x{}", "22".repeat(32)),
                    }),
                    "eth_getCode" => json!("0x60006000"),
                    "eth_call" => {
                        let call_data = request
                            .pointer("/params/0/data")
                            .and_then(Value::as_str)
                            .context("mock eth_call has no data")?;
                        if call_data != format!("0x{GET_EPOCH_ROOT_SELECTOR}{}", "33".repeat(32)) {
                            bail!("unexpected getEpochRoot call data")
                        }
                        if request.pointer("/params/1").and_then(Value::as_str) != Some("0x2") {
                            bail!("eth_call was not pinned to the fixed block")
                        }
                        json!(returned_root)
                    }
                    _ => bail!("unexpected mock method {method}"),
                };
                methods.push(method);
                let response_body = serde_json::to_vec(&json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": result,
                }))?;
                let mut writer = stream;
                write!(
                    writer,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    response_body.len()
                )?;
                writer.write_all(&response_body)?;
                writer.flush()?;
            }
            Ok(methods)
        });
        (format!("http://{address}"), handle)
    }

    #[test]
    fn chain_options_are_all_or_none() {
        assert!(optional_chain_preflight_config(None, None, None, None)
            .unwrap()
            .is_none());
        assert!(optional_chain_preflight_config(
            Some("http://127.0.0.1:8545".to_owned()),
            None,
            Some("2".to_owned()),
            None,
        )
        .is_err());
        assert!(optional_chain_preflight_config(None, None, None, Some(31337)).is_err());
        assert!(optional_chain_preflight_config(
            Some("http://127.0.0.1:8545".to_owned()),
            Some(format!("0x{}", "11".repeat(20))),
            Some("0x02".to_owned()),
            Some(31337),
        )
        .unwrap()
        .is_some());
    }

    #[test]
    fn only_fixed_numeric_block_tags_are_accepted() {
        assert_eq!(normalize_fixed_block_tag("2").unwrap(), "0x2");
        assert_eq!(normalize_fixed_block_tag("0x02").unwrap(), "0x2");
        for tag in ["latest", "safe", "finalized", "pending", "earliest", ""] {
            assert!(normalize_fixed_block_tag(tag).is_err(), "accepted {tag}");
        }
    }

    #[test]
    fn empty_contract_code_is_rejected() {
        for code in ["0x", "0x0", "0x00", "0x0000"] {
            assert!(validate_contract_code(code).is_err(), "accepted {code}");
        }
        assert_eq!(validate_contract_code("0x6000").unwrap(), 2);
    }

    #[test]
    fn http_response_parser_accepts_content_length_and_chunked() {
        let fixed = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}";
        assert_eq!(parse_http_response(fixed).unwrap(), b"{}");
        let chunked = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n{}\r\n0\r\n\r\n";
        assert_eq!(parse_http_response(chunked).unwrap(), b"{}");
    }

    #[test]
    fn rpc_evidence_url_redacts_query_credentials() {
        let endpoint = HttpEndpoint::parse("http://127.0.0.1:8545/rpc?token=secret").unwrap();
        assert_eq!(endpoint.path, "/rpc?token=secret");
        assert_eq!(endpoint.display_url, "http://127.0.0.1:8545/rpc?<redacted>");
        let root_query = HttpEndpoint::parse("http://127.0.0.1:8545?token=secret").unwrap();
        assert_eq!(root_query.path, "/?token=secret");
        assert_eq!(root_query.display_url, "http://127.0.0.1:8545/?<redacted>");
    }

    #[test]
    fn fresh_fixed_block_preflight_accepts_only_the_manifest_root() {
        let epoch_id = format!("0x{}", "33".repeat(32));
        let root = format!("0x{}", "44".repeat(32));
        let (rpc_url, server) = start_mock_rpc(root.clone());
        let evidence = read_bound_residual_epoch_root(
            &ChainPreflightConfig {
                rpc_url,
                registry_address: format!("0x{}", "11".repeat(20)),
                block_tag: "2".to_owned(),
                expected_chain_id: Some(31337),
            },
            &epoch_id,
            &root,
        )
        .unwrap();
        assert_eq!(evidence.chain_id, 31337);
        assert_eq!(evidence.block_number, 2);
        assert_eq!(evidence.contract_code_bytes, 4);
        assert_eq!(evidence.epoch_root, root);
        assert_eq!(
            server.join().unwrap().unwrap(),
            [
                "eth_chainId",
                "eth_getBlockByNumber",
                "eth_getCode",
                "eth_call"
            ]
        );
    }

    #[test]
    fn fresh_fixed_block_preflight_rejects_a_different_root() {
        let (rpc_url, server) = start_mock_rpc(format!("0x{}", "55".repeat(32)));
        let error = read_bound_residual_epoch_root(
            &ChainPreflightConfig {
                rpc_url,
                registry_address: format!("0x{}", "11".repeat(20)),
                block_tag: "0x2".to_owned(),
                expected_chain_id: Some(31337),
            },
            &format!("0x{}", "33".repeat(32)),
            &format!("0x{}", "44".repeat(32)),
        )
        .unwrap_err();
        assert!(error.to_string().contains("differs from verified"));
        server.join().unwrap().unwrap();
    }
}

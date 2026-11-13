import fs from 'node:fs';
import crypto from 'node:crypto';
import solc from 'solc';
const source = fs.readFileSync(new URL('../../contracts/EpochRootRegistry.sol', import.meta.url), 'utf8');
const input = {language:'Solidity',sources:{'EpochRootRegistry.sol':{content:source}},settings:{evmVersion:'shanghai',optimizer:{enabled:true,runs:200},outputSelection:{'*':{'*':['abi','evm.bytecode.object','evm.deployedBytecode.object']}}}};
const output = JSON.parse(solc.compile(JSON.stringify(input)));
if ((output.errors||[]).some(e=>e.severity==='error')) throw Error(JSON.stringify(output.errors));
const c=output.contracts['EpochRootRegistry.sol'].EpochRootRegistry;
fs.writeFileSync(new URL('./registry_artifact.rebuilt.json',import.meta.url),JSON.stringify({compiler:solc.version(),settings:input.settings,source,source_sha256:crypto.createHash('sha256').update(source).digest('hex'),abi:c.abi,bytecode:'0x'+c.evm.bytecode.object,runtime_template:'0x'+c.evm.deployedBytecode.object},null,2),{flag:'wx'});

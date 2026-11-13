                                                                           
#include "adapt.h"
#include "persist.h"
#include "nlohmann/json.hpp"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <chrono>
using nlohmann::json;
namespace fs=std::filesystem;
using Clock=std::chrono::steady_clock;
void need(bool x,const char* m){if(!x)throw std::runtime_error(m);}
void fresh(const std::string& p){need(!fs::exists(p),"refuse overwrite");}
AtomicSelectQuery request(const std::string& types){
 AtomicSelectQuery q;std::vector<std::string> v;std::string s;
 for(char c:types+","){if(c==','){need(s=="flashloan"||s=="swap"||s=="borrow"||s=="borrow_positive"||s=="borrow_negative"||s=="borrow_zero","unsupported type");v.push_back("type="+s);s.clear();}else s+=c;}
 q.keywordsCNF={v};return q;
}
int main(int argc,char**argv){try{
 need(argc==6,"mode input trusted types output");std::string mode=argv[1],input=argv[2],trusted=argv[3],types=argv[4],output=argv[5];
 auto q=request(types);auto start=Clock::now();
 if(mode=="seal"){
  fresh(output);fresh(trusted);TxnPool pool;std::ifstream f(input);need(bool(f),"input open");std::string line;uint32_t prev=0;
  while(std::getline(f,line)){auto j=json::parse(line);uint32_t block=j.at("mbft_block");need(block>=prev&&block>0,"block ordering");if(block!=prev){pool.blkIndex.push_back(pool.pool.size());prev=block;}pool.pool.push_back(transactionFromPayload(line,0));}
  need(!pool.pool.empty(),"empty seal");ParamBF p(0,0,.001,0);Chain c(p,0,{MAX},0,false,MBFT);c.buildChain(pool);
  saveMBFTChain(c,output);std::vector<RootCommitment> roots;SelectionMetrics m;
  selectAllChain(c,q,&roots,&m);saveCommitments(roots,trusted);
  std::cout<<json{{"rows",pool.pool.size()},{"seconds",std::chrono::duration<double>(Clock::now()-start).count()},{"native_hash","MurmurHash32"}}<<std::endl;
 }else if(mode=="prove"){
  fresh(output);auto c=loadMBFTChain(input);auto loaded=Clock::now();std::vector<RootCommitment> roots;SelectionMetrics m;auto p=selectAllChain(*c,q,&roots,&m);auto proved=Clock::now();saveAtomicSelectionProof(p,output);
  std::cout<<json{{"load_seconds",std::chrono::duration<double>(loaded-start).count()},{"proof_write_seconds",std::chrono::duration<double>(Clock::now()-loaded).count()},{"proof_compute_seconds",std::chrono::duration<double>(proved-loaded).count()},{"proof_encode_write_seconds",std::chrono::duration<double>(Clock::now()-proved).count()},{"bytes",fs::file_size(output)}}<<std::endl;
 }else if(mode=="verify"){
  fresh(output);auto p=loadAtomicSelectionProof(input);need(p.query.lowerBound==q.lowerBound&&p.query.upperBound==q.upperBound&&p.query.keywordsCNF==q.keywordsCNF,"query substitution");
  auto roots=loadCommitments(trusted);auto decoded=Clock::now();auto v=verifyAtomicSelection(p,roots);need(v.valid,"invalid proof");auto verified=Clock::now();
  std::ofstream f(output,std::ios::binary);need(bool(f),"output open");
  const bool binary=fs::path(output).extension()==".bin";
  auto put64=[&](uint64_t x){char b[8];for(int i=0;i<8;++i)b[i]=char((x>>(8*i))&255);f.write(b,8);};
  if(binary){f.write("DFROWS4\0",8);put64(v.payloads.size());}
  for(auto&s:v.payloads){auto j=json::parse(s);need(j.contains("row")&&j["row"].size()==9,"DeFi schema");
   if(binary){for(auto& x:j["row"]){need(x.is_number_integer(),"noninteger event field");put64(static_cast<uint64_t>(x.get<int64_t>()));}}
   else f<<j["row"].dump()<<'\n';}
  f.flush();need(bool(f),"output write failed");
  std::cout<<json{{"accepted",true},{"rows",v.payloads.size()},{"context_and_proof_read_seconds",std::chrono::duration<double>(decoded-start).count()},{"native_verify_seconds",std::chrono::duration<double>(verified-decoded).count()},{"payload_export_seconds",std::chrono::duration<double>(Clock::now()-verified).count()},{"read_verify_write_seconds",std::chrono::duration<double>(Clock::now()-start).count()}}<<std::endl;
 }else throw std::runtime_error("unknown mode");return 0;
 }catch(const std::exception&e){std::cerr<<"error: "<<e.what()<<std::endl;return 2;}}

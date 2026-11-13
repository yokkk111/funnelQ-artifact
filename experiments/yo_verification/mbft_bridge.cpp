                                                                               
#include "adapt.h"
#include "persist.h"
#include "nlohmann/json.hpp"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <chrono>
using nlohmann::json;namespace fs=std::filesystem;
void need(bool x,const char*m){if(!x)throw std::runtime_error(m);}
AtomicSelectQuery request(const std::string& s){
 need(s=="aggregator_intent,pool_swap,token_transfer","unsupported request");
 AtomicSelectQuery q;q.keywordsCNF={{"type=aggregator_intent","type=pool_swap","type=token_transfer"}};return q;
}
int main(int argc,char**argv){try{
 need(argc==6,"mode input trusted types output");std::string mode=argv[1],input=argv[2],trusted=argv[3],output=argv[5];
 auto q=request(argv[4]);need(!fs::exists(output),"refuse overwrite");auto start=std::chrono::steady_clock::now();
 if(mode=="seal"){
  need(!fs::exists(trusted),"refuse root overwrite");TxnPool pool;std::ifstream f(input);need(bool(f),"source open");std::string line;uint32_t prev=0;
  while(std::getline(f,line)){auto j=json::parse(line);uint32_t block=j.at("mbft_block");need(block>=prev&&block>0,"block order");if(block!=prev){pool.blkIndex.push_back(pool.pool.size());prev=block;}pool.pool.push_back(transactionFromPayload(line,0));}
  need(!pool.pool.empty(),"empty input");ParamBF p(0,0,.001,0);Chain c(p,0,{MAX},0,false,MBFT);c.buildChain(pool);saveMBFTChain(c,output);
  std::vector<RootCommitment> roots;SelectionMetrics m;selectAllChain(c,q,&roots,&m);saveCommitments(roots,trusted);
 }else if(mode=="prove"){
  auto c=loadMBFTChain(input);std::vector<RootCommitment> roots;SelectionMetrics m;auto p=selectAllChain(*c,q,&roots,&m);saveAtomicSelectionProof(p,output);
 }else if(mode=="verify"){
  auto p=loadAtomicSelectionProof(input);need(p.query.lowerBound==q.lowerBound&&p.query.upperBound==q.upperBound&&p.query.keywordsCNF==q.keywordsCNF,"query substitution");
  auto roots=loadCommitments(trusted);auto v=verifyAtomicSelection(p,roots);need(v.valid,"invalid proof");std::ofstream f(output);
  for(auto&s:v.payloads){auto j=json::parse(s);need(j.contains("row")&&j["row"].size()==11,"YO schema");auto k=j["row"][2].get<int>();need(k>=0&&k<3,"unexpected kind");std::string names[]={"aggregator_intent","pool_swap","token_transfer"};need(j["event_type"]==names[k],"payload category mismatch");f<<j["row"].dump()<<'\n';}
 }else throw std::runtime_error("unknown mode");
 std::cout<<json{{"accepted",true},{"seconds",std::chrono::duration<double>(std::chrono::steady_clock::now()-start).count()},{"bytes",fs::file_size(output)}}<<std::endl;
 return 0;
 }catch(const std::exception&e){std::cerr<<e.what()<<std::endl;return 2;}}

#include <cstdlib>
#include <sstream>
#include <algorithm>
#include <cassert>
#include <cmath>
#include <fstream>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string.h>

using namespace std;

// colors
const std::string red("\033[0;31m");
const std::string green("\033[1;32m");
const std::string yellow("\033[1;33m");
const std::string cyan("\033[0;36m");
const std::string magenta("\033[0;35m");
const std::string reset("\033[0m");

struct Graph
{
  vector<vector<int> > graph_;
  vector<vector<int> > graph_transpose_;
  vector<int> indegree_;
  vector<int> outdegree_;  
  vector<string> original_ids_;
  unordered_map<string, int> id;
  int size_;
  void addEdge(int from, int to)
  {
    while (from >= size_ or to >= size_) {
      graph_.push_back(vector<int>());
      graph_transpose_.push_back(vector<int>());
      indegree_.push_back(0);
      outdegree_.push_back(0);
      ++size_;
    }
    outdegree_[from]++;
    indegree_[to]++;
    graph_[from].push_back(to);
    graph_transpose_[to].push_back(from); 
  }
};


Graph read_graph(istream & input, bool directed)
{
  Graph g;
  g.size_ = 0;
  int n = 0;

  cout << "reading graph...";
  std::string line;
  while(std::getline(input, line)) {     
    std::istringstream iss(line);
    std::string token;
    int from, to;
    string sfrom, sto;
    int fieldCount = 0;
    while(std::getline(iss, token, '\t'))  {
	if (fieldCount == 0) sfrom = token;
        if (fieldCount == 1) sto = token;
	fieldCount++;	
    }
	
    if (g.id.count(sfrom)) from = g.id[sfrom];
    else from = g.id[sfrom] = n++;

    if (g.id.count(sto)) to = g.id[sto];
    else to = g.id[sto] = n++;

    g.addEdge(from, to);
    if (!directed)
      g.addEdge(to, from);
  }

  g.original_ids_.resize(g.id.size());
  
  for (auto it : g.id) {
    g.original_ids_[it.second] = it.first;
  }
  cout << "Graph has been read." << "\n";
  return g;
}           

class TrustRank 
{
  public:
    TrustRank(Graph & graph, double d, int nthreads = 16)
    : graph_(graph),
      d_(d),
      nthreads_(nthreads)
    {
      trust_rank_[0] = vector<double>(graph.size_);
      trust_rank_[1] = vector<double>(graph.size_);
    }

    virtual ~TrustRank()
    {
    }

    bool compute_trust_rank(std::function<bool(int)> is_seed, 
                             double error_tolerance = 1e-14,
                             int max_iterations = 500)
    {
      const vector<int> zero_degree_nodes = 
                        compute_zero_degree_nodes();
      const int n = graph_.size_;
      const int nseeds = count_seeds(is_seed);
      flag_ = true;

      // cout << "# zero degree nodes: " << zero_degree_nodes.size() << "\n";     
 
      for (int i = 0; i < n; ++i)
        trust_rank_[flag_][i] = is_seed(i) ? 1.0 / nseeds : 0.0; 

      for (int iteration = 0; 
               iteration < max_iterations; 
               ++iteration) 
      {
        const vector<double> & current = trust_rank_[flag_];
        vector<double> & next = trust_rank_[!flag_];

	cout << "\r" << "iteration " << red << iteration << reset << std::flush;
        vector<thread> threads;
        for (int tid = 0; tid < nthreads_; ++tid) {
          auto thread_function = [=, &current, &next]() {
            for (int i = tid; i < n; i += nthreads_) {
              next[i] = is_seed(i) ? (1. - d_) / nseeds : 0.0;
              for (const int & backlink : graph_.graph_transpose_[i])
                next[i] += d_ * (current[backlink] / 
                                 graph_.outdegree_[backlink]);
              for(const int & backlink : zero_degree_nodes) 
                if (i != backlink)
                  next[i] += d_ * (current[backlink] / (n - 1.0));
            }
          };
          threads.push_back(thread(thread_function));
        }
        for (auto & t : threads) t.join();
        flag_ = !flag_;

        if (compute_error() < error_tolerance)
          return true;
      } 
      assert(false);
      return false;
    }

    vector<double> trust_rank() const {
      return trust_rank_[flag_];
    }

  private:
    Graph & graph_;
    vector<double> trust_rank_[2];
    double d_;
    bool flag_;
    int nthreads_;

    double compute_error()
    {
      double error = 0.0;
      for (int i = 0; i < graph_.size_; ++i) 
        error += powf(trust_rank_[0][i] - trust_rank_[1][i], 2.0);
      error /= graph_.size_;
      error = sqrt(error);
      // cout << "\n\nerror: " << error;
      return error;
    }

    vector<int> compute_zero_degree_nodes() const
    {
      const int n = graph_.size_;
      vector<int> zero_degree_nodes;
      for (int i = 0; i < n; ++i) 
        if (graph_.outdegree_[i] == 0)
          zero_degree_nodes.push_back(i);
      return zero_degree_nodes; 
    }

    int count_seeds(function<bool(int)> is_seed) const
    {
      const int n = graph_.size_;
      int nseeds = 0;
      for (int i = 0; i < n; ++i) 
        if (is_seed(i))
          ++nseeds;
      return nseeds;
    }

 
};

unordered_set<int> read_seeds(Graph & g, std::istream & input)
{
  cout << "reading seeds..." << endl;
  unordered_set<int> seeds;
  string node_name;
  vector <string> seedsFound;
  vector <string> seedsNotFound;
  int foundSeedsCount = 0;
  int notFoundSeedsCount = 0;	
  while (true) {
    getline(input, node_name);
    if (!input) break;
    if (g.id.count(node_name)) {
      seeds.insert(g.id[node_name]);
      seedsFound.push_back(node_name);	
      foundSeedsCount++;	
    } else {
      notFoundSeedsCount++;
      seedsNotFound.push_back(node_name);		
    }
  }
  cout << "seeds found: " << foundSeedsCount << endl;
  for (const auto& i: seedsFound)
    std::cout << i << ' ';
  cout << endl << endl;

  cout << "seeds not found: " << notFoundSeedsCount << endl;
  for (const auto& i: seedsNotFound)
    std::cout << i << ' ';
  cout << endl << endl;

  return seeds;
}

void output_rank(Graph & g, vector<double> & rank, ostream & output, string side, float dumping_factor)
{
  // cout << "outputting ranks... " << endl;
  // cout << "rank size: " << rank.size() << endl;
  // sort ranks and keep track of indexes
  vector<long> indexes(rank.size());

  std::size_t n(0); 
  // cout << "will generate\n";
  std::generate(std::begin(indexes), std::end(indexes), [&]{ return n++; });
  // cout << "will sort\n";
  std::sort(  std::begin(indexes), 
                std::end(indexes),
                [&](long i1, long i2) { return rank[i1] > rank[i2]; } );

  long rank_position = 1;
  // cout << "foreach index... " << endl;
  for (auto v : indexes) {
    output << g.original_ids_[v].c_str() << '\t' << side << '\t';
    output << left << setfill('0') << setw(4) << dumping_factor; 
    output << '\t' << rank[v] << '\t' << rank_position << endl;
    rank_position++;
  }
}

int main(int argc, const char * argv[])
{
  assert(argc == 7);
  ifstream graph_input(argv[1]);
  ifstream seed_input(argv[2]);
  ofstream rank_output(argv[5]);
  bool directed = strcmp(argv[6], "1") == 0;

  cout << "is graph directed? " << directed << "\n";

  if (!graph_input) {
   cout << "graph input " << argv[1] << " does not exist.\n";
   exit(0); 
  }
  /*
  if (!seed_input) {
   cout << "seed input " << argv[2] << " does not exist.\n";
   exit(0); 
  } */

  double dumping_factor = atof(argv[3]);
  cout << "dumping factor: ";
  cout << left << setfill('0') << setw(4) << dumping_factor << "\n"; 

  Graph g = read_graph(graph_input, directed);
  cout << "graph size: " << g.size_ << "\n";
  TrustRank pr(g, dumping_factor);
  string side = argv[4];

  cout << "side: " << side << "\n";
  cout << std::flush;

  if (argc == 7 && side != "GLOBAL") {
    cout << "PERSONALIZED PageRank " << endl;
    ifstream seed_input(argv[2]);
    unordered_set<int> seeds = read_seeds(g, seed_input);
   
    pr.compute_trust_rank([&](int i) -> bool { return seeds.count(i); });
    vector<double> personalized_rank = pr.trust_rank();  
    // cout << endl << "rank is computed!" << endl;
    output_rank(g, personalized_rank, rank_output, side, dumping_factor);
  } else {
    // cout << "GLOBAL PageRank" << endl;	
    pr.compute_trust_rank([](int i) { return true; });
    vector<double> page_rank = pr.trust_rank();
    output_rank(g, page_rank, rank_output, side, dumping_factor);
  }
  cout << endl;
}

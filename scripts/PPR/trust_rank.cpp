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

using namespace std;

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


Graph read_graph(istream & input)
{
  Graph g;
  g.size_ = 0;
  int n = 0;

  while (true) {
    int from, to;
    string sfrom, sto, sweight;
    input >> sfrom >> sto >> sweight;
    if (!input) break;


    if (g.id.count(sfrom)) from = g.id[sfrom];
    else from = g.id[sfrom] = n++;

    if (g.id.count(sto)) to = g.id[sto];
    else to = g.id[sto] = n++;

    g.addEdge(from, to);
  }

  g.original_ids_.resize(g.id.size());
  for (auto it : g.id) {
    g.original_ids_[it.second] = it.first;
  }
  return g;
}           

class TrustRank 
{
  public:
    TrustRank(Graph & graph, double d = 0.85, int nthreads = 16)
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
                             double error_tolerance = 1e-6,
                             int max_iterations = 100)
    {
      const vector<int> zero_degree_nodes = 
                        compute_zero_degree_nodes();
      const int n = graph_.size_;
      const int nseeds = count_seeds(is_seed);
      flag_ = true;
      
      for (int i = 0; i < n; ++i)
        trust_rank_[flag_][i] = is_seed(i) ? 1.0 / nseeds : 0.0; 

      for (int iteration = 0; 
               iteration < max_iterations; 
               ++iteration) 
      {
        const vector<double> & current = trust_rank_[flag_];
        vector<double> & next = trust_rank_[!flag_];

        cerr << "ITERACAO " << iteration << endl;
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
  unordered_set<int> seeds;
  string node_name;
  while (true) {
    getline(input, node_name);
    if (!input) break;
    if (g.id.count(node_name)) {
      seeds.insert(g.id[node_name]);
    } else {
      cerr << "WARNING: Seed not found in graph" << endl;
    }
  }
  return seeds;
}

void output_rank(Graph & g, vector<double> & rank, ostream & output)
{
  for (int i = 0; i < rank.size(); ++i) {
    output << g.original_ids_[i].c_str() << '\t' << rank[i] << endl;
  }
}

int main(int argc, const char * argv[])
{
  ifstream graph_input(argv[1]);
  Graph g = read_graph(graph_input);
  TrustRank pr(g);

  if (argc == 3) {
    ifstream seed_input(argv[2]);
    unordered_set<int> seeds = read_seeds(g, seed_input);
    pr.compute_trust_rank([&](int i) -> bool { return seeds.count(i); });
    vector<double> personalized_rank = pr.trust_rank();  
    output_rank(g, personalized_rank, cout);
  }else{
    pr.compute_trust_rank([](int i) { return true; });
    vector<double> page_rank = pr.trust_rank();
    output_rank(g, page_rank, cout);
  }

}

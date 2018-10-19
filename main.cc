#include <iostream>
#include "kvindex.h"

using namespace std;

int main() {
  KVIndex kv;
  cout << "Loading..." << endl;
  kv.Load("data.dat");
  cout << "Loading completed" << endl;
  cout << "jkvLfNTuJejW4x8jqVNymd" << " " << kv.Get("jkvLfNTuJejW4x8jqVNymd") << endl;
  cout << "exit" << endl;
  return 0;
}

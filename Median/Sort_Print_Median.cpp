/*
File Name: Sort_Print_Median.cpp
by: Hang Chu, Cornell University
Insight Data Engineer Coding Challenge

Serial code for computing moving median,
using the result of word count per line produced by Hadoop MapReduce
*/
#include <iostream>
#include <algorithm>
#include <vector>
#include <string>
#include <fstream>

using namespace std;

/*
Basic struct for one line produced by Hadoop MapReduce
byte_offset: byte offset of a line to the beginning of its file
file_name: the file name
word_num: number of words in a line 
*/
struct byte_file_num
{
	long byte_offset;
	string file_name;
	long word_num;
};

/*
The comparor of the byte_file_num struct,
used in vector sorting.
*/
bool bfn_comparor(byte_file_num bfn1,byte_file_num bfn2)
{
	if (bfn1.file_name==bfn2.file_name)
		return bfn1.byte_offset<bfn2.byte_offset;
	else
		return bfn1.file_name<bfn2.file_name;
}

/*
The main function: read in all triplets produced by Hadoop MapReduce
Then compute moving medians.
*/
int main(int argc, char const *argv[])
{
	ifstream ifs;
	ifs.open ("../tmp_output/part-r-00000", ifstream::in);
	vector<byte_file_num> all_bfns;
    while (!ifs.eof())
    {
    	string s;
    	long b,w;
    	byte_file_num bfn;
    	ifs>>b;
    	if (ifs.eof())
    		break;
    	ifs>>s;
    	if (ifs.eof())
    		break;
    	ifs>>w;
    	std::transform(s.begin(),s.end(),s.begin(),::tolower);
    	bfn.byte_offset=b;bfn.file_name=s;bfn.word_num=w;
    	all_bfns.push_back(bfn);
    }
	ifs.close();
	sort(all_bfns.begin(),all_bfns.end(),bfn_comparor);
	int len=all_bfns.size();
	ofstream ofs;
	ofs.open("../wc_output/med_result.txt");
	vector<long> nums;
	for (int i=0;i<len;i++)
	{
		nums.push_back(all_bfns[i].word_num);
		sort(nums.begin(),nums.end());
		cout<<endl;
		double nowmedian;
		if (i%2==0)
			nowmedian=(double)nums[i/2];
		else
			nowmedian=((double)nums[(i+1)/2]+(double)nums[(i-1)/2])/2.0;
		char ss[100];
		sprintf(ss,"%1.1f",nowmedian);
		ofs<<ss<<endl;
	}
	ofs.close();
	return 0;
}

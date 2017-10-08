# Page_rank

This Projec is to perform page rank  of google search with mapreduce.

input: 
* webpage linkage data
* initial page rank

algorithm:

Pr(k+1) = transition matrix * Pr(k)
 
Classes:

* UnitMultiplication: generate transition matrix and perform element-wise multiplication of transition matrix and pr matrix
* UnitSum: perform summation of the multiplication results group by each row


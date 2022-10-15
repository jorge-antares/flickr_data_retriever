# Flickr Data Retriever

Flickr_Manager (FM)  reads data from the
flickrapi and saves it as a csv.

Since smaller bounding boxes (BBs) yield
more observations, FM allows the user to
indicate the number of segments to divide
the BB along each axis (num_seg). Therefore
total number of subareas is num_seg^2.

Queries to flickrapi are limited to 4000
observations by the API and thereafter,
these are repeated. The Flickr_Grid_Manager
class ensures queries sent are <= 4000, 
by creating several Flickr_Data_Retriever 
objects with a proper timestep that ensures 
this constraint is met.

User inputs:
- Bounding box (BB) as a list of integers.
- Number of segments to divide each axis of the BB.
- Initial and final year of the search.
- Optional parameters to pass to the Flicker API search function.

Output:
- csv file saved on desired folder.

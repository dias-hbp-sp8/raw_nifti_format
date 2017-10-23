# NIFTI data source for RAW

The goal of the NIFTI data source extension for RAW is to open NIFTI files with RAW and access the contained data through the interface with SQL-like queries.


### Context
RAW works with various kind of data sources, usually containing a large number of records. For each data source, RAW infers the schema (structure of the data) and generates the code to parse it efficiently. The code dedicated to each format must provide some predefined functionalities, which enables new formats to be supported through independent plug-ins.

NIFTI files are an unusual data source: they contain only one object (rather than a list like a csv file for instance). This object represents medical imaging data defined by voxel values in a regular matrix of up to 7 dimensions. NIFTI-1 format files consist of a header and a data part; the file nifti1.h of the c implementation defines the header format.

The header contains mixed information regarding the size of the data, how to parse it, how to use it and its origin and meaning. Although some elements are used only for opening the file, some header fields are necessary to use the data. Given that, the header content must be made available with the imaging data.

The data itself is a large array of n dimensions with a value for each voxel. The header contains information regarding scaling of rotation that must be applied to the data.


### Functionalities
As a RAW data source plug-in, the NIFTI extension performs the following tasks:

- Infer the schema of data according to RAW requirements: varying part is the dimensionality of data and data type used.
- Register the data source with its schema.
- Generate dedicated code for reading the data.


### NIFTI as a data source type
The structure of RAW does not allow to register data sources as a unique record: a NIFTI file thus needs to be open as a list of one record. The future development of the BIDS plugin, which will allow to open patient data directories as data sources, should rationalise this by providing access to several NIFTI files at once.

NIFTI voxel values can be transformed to store the highest precision possible in the minimum possible memory space, or for any other reasons. Real voxel values are rebuilt by applying a scaling, offset and possibly rotation defined in the header. 

The scaling is automatically applied when reading the voxel values with this plugin.

### Remarks

- Only one-file NIFTI are currently supported.
The NIFTI format provides two option: having the header and data in the same file or in two different files. In the context of RAW, the first version is implemented, as the management of file access can be left to RAW and its support of various data storage systems.
The possibility to support two-files NIFTIs will be explored in a second phase.

- The current implementation will read the header to infer the schema, and read all the voxel data whenever voxel values are queried. The test NIFTI files currently available to us do not exceed 50Mo, and thus there is no incentive to try and load only part of the data. Further investigations on the possibility to do so will be performed if the need arises.


# communication_request
communication module about  client、metadata server、and node server 
specially,
client send read request(including blockID) to metadata server,metadata server send response to client the location of blockID.Then client send read request to the corresponding node server,the node server then send the context of the blockID.

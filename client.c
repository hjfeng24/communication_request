#include "common.h"

#define TraceNum 5000
int client2metadata_sockfd;   //client到metadata通信套接字
int client2node_sockfd;//client到node通信套接字
node *nodes;

int client_init();
void client2metadata_send(Client2MetadataRequest *request);
void metadata2client_recv(Client2MetadataResponse *response);
void client2node_send(Client2NodeRequest* request);

//客户端向协调器发送消息
void client2metadata_send(Client2MetadataRequest *request){
    size_t needSend=sizeof(Client2MetadataRequest); //要读取的字节总数
    char *sendBuf=(char *)malloc(needSend);  //缓冲区
    memcpy(sendBuf,request,needSend); //序列化
	/*测试*/
	Client2MetadataRequest *request2=(Client2MetadataRequest*)malloc(sizeof(Client2MetadataRequest));
	memcpy(request2,sendBuf,needSend);
	printf("request2->requestTYPE=%d,request2->blockID=%lu,request2->nodeID=%d\n",request2->requestTYPE,request2->blockID,request2->nodeID);
	printf("client2metadata_sockfd=%d\n",client2metadata_sockfd);
  int ppp= send_bytes(client2metadata_sockfd,sendBuf,needSend); //Client发送请求至Coordinator
	printf("needSend=%lu,sendBuf=%s,ppp=%d\n",needSend,sendBuf,ppp);//测试

}

//Client接收来自Metadata反馈信息（数据分块BlockID的元信息）
void metadata2client_recv(Client2MetadataResponse *response){
size_t needRecv=sizeof(Client2MetadataResponse);  //接收字节总数
char *recvBuf=(char *)malloc(needRecv);               //缓冲区
recv_bytes(client2metadata_sockfd,recvBuf,needRecv);  //接收来自coordinator的数据分块的元数据信息
memcpy(response,recvBuf,needRecv);  //反序列化
//printf("needRecv=%lu,recvBuf=%s\n",needRecv,recvBuf);//测试
}

//client向server发送读数据请求
void client2node_send(Client2NodeRequest* request)
{
	size_t needSend=sizeof(Client2NodeRequest);
	char *sendBuf=(char *)malloc(needSend);
	memcpy(sendBuf,request,needSend);//序列化
	send_bytes(client2node_sockfd,sendBuf,needSend);//Client发送请求至server
	
}
int client_init()
{
//	int client2metadata_sockfd;
	nodes=read_config("config");//读节点与ip地址的配置信息；
	char *ip="192.168.0.49";
	printf("client_init begin............................\n");
	client2metadata_sockfd = client_connect(nodes[8].ip,Client2MetadataPort); //连接协调器
	if(client2metadata_sockfd == -1) {
  	printf("client2metadata_sockfd  is error***************\n");
    return ERR;
  }
	printf("client2metada_sockfd %d succeed\n",client2metadata_sockfd);
	/*下面进行写操作*/
	//@@@@@$$$$
	for(int i=0;i<TraceNum;i++)//TraceNum;i++)
//@@@@$$$$
	{
//@@@@$$$$
		printf("----------播放第%d条请求----------\n",i+1);
//@
	
	//@@@@$$$$while(1){
//@@@@$$$$		printf("------------播放请求-----------\n");
		
		int Y=6000;
		Client2MetadataRequest *c2m_request=(Client2MetadataRequest *)malloc(sizeof(Client2MetadataRequest));
 	    Client2MetadataResponse *m2c_response=(Client2MetadataResponse *)malloc(sizeof(Client2MetadataResponse));
		
		int myrand=rand()%Y+1;//@@@@6.22号晚加 +1
		c2m_request->requestTYPE=0;               //请求ID  
   		c2m_request->blockID=myrand;  
   		c2m_request->nodeID=-1;
	//prinf("c2m_request->request")
	client2metadata_send(c2m_request);  //发送请求 
	
    metadata2client_recv(m2c_response);  //接收请求 
	printf("metadata2client blockID=%ld,nodeID=%d\n",m2c_response->blockID,m2c_response->nodeID);
	
	client2node_sockfd=client_connect(nodes[m2c_response->nodeID].ip,Client2NodePort);//连接node服务器
	if(client2node_sockfd == -1) {
  	printf("client2node_sockfd  is error***************\n");
    return ERR;
  }
	Client2NodeRequest* c2n_request=(Client2NodeRequest*)malloc(sizeof(Client2NodeRequest));
	char *recvBlock=(char*)malloc(sizeof(char)*Block_Size);
	
	c2n_request->requestTYPE=0;//0为读请求
	c2n_request->blockID=m2c_response->blockID;
	c2n_request->nodeID=m2c_response->nodeID;
	//c2n_request->blockBuf[Block_Size]={0};//给字符串赋空值
	printf("c2n_request->requestTYPE=%d,blockID=%ld,nodeID=%d,nodeIP=%s,blockBuf=%s\n",c2n_request->requestTYPE,c2n_request->blockID,c2n_request->nodeID,nodes[c2n_request->nodeID].ip,c2n_request->blockBuf);
	client2node_send(c2n_request);
	
	recv_bytes(client2node_sockfd,recvBlock,Block_Size);//接收来自node server的数据
	printf("recvBlock=%s\n",recvBlock);
	
	printf("\n");
//@@@@@@$$$$
//@@@@$$$$}
}
	//sleep(5);//@6.22晚加无用
	//close(client2metadata_sockfd);
	return 0;
}
int main()
{
	int flag;
	printf("client_init begin----\n");
	flag=client_init();
	if(flag==-1)
	{
		perror("client_init is error----\n");
		return -1;
	}
	printf("client_init succeed----\n");
	return 0;
}

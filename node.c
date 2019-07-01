#include "common.h"

int node2metadata_sockfd;//数据节点到元数据管理器通信套接字
int node2client_sockfd;//数据节点到客户端通信套接字
node *nodes;
int recvTraceNum=0;

void client2node_recv(Client2NodeRequest *request);//接收客户端的读请求
int node_init();
static int socket_bind();
static void add_event(int epollfd,int fd,int state);
static  void delete_event(int epollfd,int fd,int state);
static void modify_event(int epollfd,int fd,int state);

static void do_epoll(int listenfd);
static void handle_events(int epollfd,struct epoll_event *events,int num,int listenfd);
static void handle_accpet(int epollfd,int listenfd);
static void handle_accpet2(int epollfd,int listenfd);     

static void do_read(int epollfd,int fd);
void client2node_recv(Client2NodeRequest*request){
	size_t needRecv=sizeof(Client2NodeRequest);
	char *recvBuf=(char *)malloc(needRecv);
	recv_bytes(node2client_sockfd,recvBuf,needRecv);
	memcpy(request,recvBuf,needRecv);//反序列化
}



//创建套接字并进行绑定
static int socket_bind()
{
	int listenfd;
	struct sockaddr_in servaddr;
	listenfd=socket(AF_INET,SOCK_STREAM,0);
	if(listenfd==-1)
	{
		perror("socket error----\n");
		exit(1);
	}
	bzero(&servaddr,sizeof(servaddr));
	servaddr.sin_family=AF_INET;
	servaddr.sin_addr.s_addr=htonl(INADDR_ANY);
	servaddr.sin_port=htons(Client2NodePort);
	
	int bind_flag=bind(listenfd,(struct sockaddr*)&servaddr,sizeof(servaddr));
	if(bind_flag==-1)
	{
		perror("bind error----\n");
		exit(1);
	}
	return listenfd;
}

/*添加事件*/
static void add_event(int epollfd,int fd,int state)
{
	struct epoll_event ev;
	ev.events=state;
	ev.data.fd=fd;
	epoll_ctl(epollfd,EPOLL_CTL_ADD,fd,&ev);
	
}
/*删除事件*/
static  void delete_event(int epollfd,int fd,int state)
 {
        struct epoll_event ev;
        ev.events = state;
        ev.data.fd = fd;
       // epoll_ctl(epollfd,EPOLL_CTL_DEL,fd,&ev);
	   int res;
		if((res=epoll_ctl(epollfd,EPOLL_CTL_DEL,fd,&ev))<0)
		      {
		          perror("epoll_ctl_del error\n");
		      }
}

/*修改事件读写状态*/
static void modify_event(int epollfd,int fd,int state)
{
       struct epoll_event ev;
       ev.events = state;
       ev.data.fd = fd;                  
       epoll_ctl(epollfd,EPOLL_CTL_MOD,fd,&ev);
}       

static void do_epoll(int listenfd)
{
	int efd;
	int ret;
	struct epoll_event ep[OPEN_MAX];
	//创建一个红黑树描述符
	efd=epoll_create(OPEN_MAX);//创建epoll模型，efd指向红黑树根节点
	//添加监听描述符事件
	add_event(efd,listenfd,EPOLLIN);
	for( ; ; )
	{
		//获取已经准备好的描述符事件
           ret = epoll_wait(efd,ep,OPEN_MAX,-1);
           handle_events(efd,ep,ret,listenfd);
}
	close(listenfd);
//	close(efd);
}
static void handle_events(int epollfd,struct epoll_event *events,int num,int listenfd)
{
	int i;
	int fd;
	//Client2MetadataRequest *read_return_request=(Client2MetadataRequest*)malloc(sizeof(Client2MetadataRequest));
     //进行选好遍历
     for (i = 0;i < num;i++)
       {
          fd = events[i].data.fd;
          //根据描述符的类型和事件类型进行处理
		  if(!(events[i].events&EPOLLIN))//@如果不是读事件，继续循环
		  	continue;
          if (fd == listenfd) 
                handle_accpet(epollfd,listenfd);
		

 
          else if (events[i].events & EPOLLIN)
		  {
		  		//read_return_request=(Client2MetadataRequest*)malloc(sizeof(Client2MetadataRequest)); 
                //read_return_request =
				do_read(epollfd,fd);
			}
         // else if (events[i].events & EPOLLOUT)
           //       do_write(epollfd,fd,read_return_request);
       }         
	
}

//添加与监听事件连接的客户端clifd到红黑树中
static void handle_accpet(int epollfd,int listenfd)
 {
        int clifd;
        struct sockaddr_in cliaddr;
        socklen_t cliaddrlen;
        clifd = accept(listenfd,(struct sockaddr*)&cliaddr,&cliaddrlen);
        if (clifd == -1)
                perror("accpet error:\n");
        else
           {
           printf("accept a new client: %s:%d\n",inet_ntoa(cliaddr.sin_addr),cliaddr.sin_port);
           //添加一个客户描述符和事件
           add_event(epollfd,clifd,EPOLLIN);//默认为读事件
           }
 } 
 //添加与监听事件连接的客户端clifd到红黑树中
 
static void do_read(int epollfd,int fd) 
{
	node2client_sockfd=fd;
	printf("node2client_sockfd=%d\n",node2client_sockfd);
	/*下面进行读操作*/
	printf("node server read begin----\n");
	Client2NodeRequest *c2n_request=(Client2NodeRequest *)malloc(sizeof(Client2NodeRequest));
	//	 printf("metadata_manage()======2\n" );
	client2node_recv(c2n_request);  //接收到客户端的信息
	//@@@@@@@@@@@@@@@
	printf("我是测试c2n_request->blockID=%ld\n",c2n_request->blockID);
	if(c2n_request->blockID==0)
	{
		printf("111111111111111111111111111-------\n");	
		int res;
//		if((res=epoll_ctl(efd,EPOLL_CTL_DEL,node2client_sockfd,NULL))<0)
//		{
//			perror("epoll_ctl_del error\n");
//		}
		printf("我从树上摘内容了,要关闭node2client_sockfd=%d\n",node2client_sockfd);
		delete_event(epollfd,fd,EPOLLIN);//EPOLLIN|EPOLLOUT);
		close(node2client_sockfd);//关闭这个客户端套接字，不然会一直占用
	}
	else if(c2n_request->blockID>0){//实际读到了内容
		recvTraceNum++;
		printf("------服务器接收到第%d条请求-----\n",recvTraceNum);
		char *c2n_ReadOrWrite;
		if(c2n_request->requestTYPE==0)
			c2n_ReadOrWrite="读请求";
		else
			c2n_ReadOrWrite="写请求";
		printf("client send to node msg requestTYPE=%s,blockID=%ld,nodeID=%d,nodeIP=%s,blockBuf=%s\n",c2n_ReadOrWrite,c2n_request->blockID,c2n_request->nodeID,nodes[c2n_request->nodeID].ip,c2n_request->blockBuf);
		printf("blockID=%ld,StripeID=%ld\n",c2n_request->blockID,(c2n_request->blockID)/nodeInStripe);
    	/*给客户端*/
		char blockBufMem[Block_Size]={0};
		printf("blockBufMem=%s\n",blockBufMem);

		get_rand_str(blockBufMem,Block_Size);
		printf("blockBufMem=%s\n",blockBufMem);
		send_bytes(node2client_sockfd,blockBufMem,Block_Size);
		
	}
}	
int node_init(){
	int listenfd;
	listenfd=socket_bind();
	listen(listenfd,Max_Length_backlog);
	
	int opt=1;
	if(setsockopt(listenfd,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt))<0){//设置端口复用
		perror("setsocket\n");
		exit(1);
	}
	
	do_epoll(listenfd);
	return 0;
}
int main()
{	
	printf("Max_Wait_Length=%d\n",Max_Length_backlog);
	/*读取配置文件信息*/
	nodes=read_config("config");//
	if(nodes==NULL)
	{
		printf("read config is error----\n");
		return ERR;
	}
	int flag1;
	flag1=node_init();
	if(flag1==-1)
	{
		printf("node init is error\n");
		return ERR;
	}
	printf("node_init succeed----\n");
	return 0;
}



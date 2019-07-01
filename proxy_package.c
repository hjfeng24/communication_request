#include"common.h"
node *nodes;//全局配置文件节点信息
int  SERVER_ACCEPT_REQUEST_NUM=0;
int recvTraceNum=0;
int metadata2client_sockfd;

Client2MetadataRequest *read_return_request=NULL;//=(Client2MetadataRequest*)malloc(sizeof(Client2MetadataRequest)); 

void client2metadata_recv(Client2MetadataRequest *request);
void metadata2client_send(Client2MetadataResponse *response);
int metadata_init();
static int socket_bind();
static void add_event(int epollfd,int fd,int state);
static  void delete_event(int epollfd,int fd,int state);
static void modify_event(int epollfd,int fd,int state);

static void do_epoll(int listenfd);
static void handle_events(int epollfd,struct epoll_event *events,int num,int listenfd);
static void handle_accpet(int epollfd,int listenfd);
static void handle_accpet2(int epollfd,int listenfd);     

static void do_read(int epollfd,int fd);


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
	servaddr.sin_port=htons(Client2MetadataPort);
	
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
	metadata2client_sockfd=fd;
	printf("metadata2client_sockfd=%d\n",metadata2client_sockfd);
	/*下面进行读操作*/
	printf("metadata server read begin----\n");
	Client2MetadataRequest *c2m_request=(Client2MetadataRequest *)malloc(sizeof(Client2MetadataRequest));
	//	 printf("metadata_manage()======2\n" );
	client2metadata_recv(c2m_request);  //接收到客户端的信息
	//@@@@@@@@@@@@@@@
	printf("我是测试c2m_request->blockID=%ld\n",c2m_request->blockID);
	if(c2m_request->blockID==0)
	{
		printf("111111111111111111111111111-------\n");	
		int res;
//		if((res=epoll_ctl(efd,EPOLL_CTL_DEL,metadata2client_sockfd,NULL))<0)
//		{
//			perror("epoll_ctl_del error\n");
//		}
		printf("我从树上摘内容了,要关闭metadata2client_sockfd=%d\n",metadata2client_sockfd);
		delete_event(epollfd,fd,EPOLLIN);//EPOLLIN|EPOLLOUT);
		close(metadata2client_sockfd);//关闭这个客户端套接字，不然会一直占用
	}
	else if(c2m_request->blockID>0){//实际读到了内容
		recvTraceNum++;
		printf("------元数据管理器接收到第%d条请求-----\n",recvTraceNum);
		char *c2m_ReadOrWrite;
		if(c2m_request->requestTYPE==0)
			c2m_ReadOrWrite="读请求";
		else
			c2m_ReadOrWrite="写请求";
		printf("client send to metadata msg requestTYPE=%s,blockID=%ld,nodeID=%d\n",c2m_ReadOrWrite,c2m_request->blockID,c2m_request->nodeID);
	
    	/*给客户端*/
		Client2MetadataResponse *c2m_response=(Client2MetadataResponse *)malloc(sizeof(Client2MetadataResponse));
		c2m_response->blockID=c2m_request->blockID;//6.22晚加
		printf("测试：nodeInStripe=%d\n",nodeInStripe);
		printf("c2m_request->blockID/nodeInStripe=%ld,c2m_request->blockID)%%nodeInStripe=%ld\n",((c2m_request->blockID)-1)/nodeInStripe,((c2m_request->blockID)-1)%nodeInStripe);
		int mynodeid=node2blockTest[(c2m_request->blockID)/nodeInStripe][(c2m_request->blockID)%nodeInStripe];
		c2m_response->nodeID=mynodeid;//(c2m_request->blockID)%6;
		printf("metadata send to client msg blockID=%ld,nodeID=%d\n",c2m_response->blockID,c2m_response->nodeID);   
		for(int i=0;i<6;i++)
			{
				if(mynodeid==nodes[i].nodeID)
				{
					printf("------nodeIP=%s-----\n",nodes[i].ip);
					break;
				}
			}

	 metadata2client_send(c2m_response);



	}
}	


void client2metadata_recv(Client2MetadataRequest *request){  //接收来自客户端的消息
  //printf("client2metadata_recv=====================\n" );
    size_t needRecv=sizeof(Client2MetadataRequest);
	printf("测试：metadata2client_sockfd=%d\n",metadata2client_sockfd);
    char *recvBuf=(char *)malloc(needRecv);   
	recv_bytes(metadata2client_sockfd,recvBuf,needRecv); //接收来自client的数据信息
	memcpy(request,recvBuf,needRecv);  //将recvBuf反序列化存到request中
		}

void metadata2client_send(Client2MetadataResponse *response){  //协调器向客户端发送数据分块信息
	  size_t needSend = sizeof(Client2MetadataResponse);
	  char *sendBuf=(char *)malloc(needSend);
	  memcpy(sendBuf,response,needSend);
	  send_bytes(metadata2client_sockfd,sendBuf,needSend);
	  }
int metadata_init(){
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
	/*读取块到节点的信息---随机分布*/
	ReadNode2BlockFile("node2block.txt");
	/*读取配置文件信息*/
	nodes=read_config("config");//
	if(nodes==NULL)
	{
		printf("read config is error----\n");
		return ERR;
	}
	int flag1;
	flag1=metadata_init();
	if(flag1==-1)
	{
		printf("metadata init is error\n");
		return ERR;
	}
	printf("metadata_init succeed----\n");
	return 0;
}

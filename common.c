#include"common.h"
#include<arpa/inet.h>
#include<unistd.h>
#include<netdb.h>
#include<stdio.h>
#include<stdlib.h>
#include<errno.h>
#include<sys/epoll.h>
#include<string.h>
#include<ctype.h>
#include<netinet/tcp.h>//for TCP_NODELAY
#define OPEN_MAX 20//struct epoll_event类型数组ep的数组容量,OPEN_MAX为数组容量
#define MAX_LINE 8192//字符串缓冲区大小   
#define COMMENTS '#'
int client_connect(char *ip,ushort port)//客户端连接元数据管理器、服务器
{	
	int sockfd;
	printf("client connect %s try begin----\n",ip);
	
	/*步骤1：创建socket*/
	sockfd=socket(AF_INET,SOCK_STREAM,0);
	if(sockfd<0)
	{
		perror("socket error\n");
		exit(1);
	}
	struct sockaddr_in serveraddr;
	memset(&serveraddr,0,sizeof(serveraddr));
	serveraddr.sin_family=AF_INET;
	serveraddr.sin_addr.s_addr=inet_addr(ip);
	serveraddr.sin_port=htons(port);
	/*步骤2:客户端用connect函数连接到服务器*/
	if(0==connect(sockfd,(struct sockaddr*)&serveraddr,sizeof(serveraddr)))//客户端成功连接
	{	
		int flag=1;
		if(setsockopt(sockfd,SOL_SOCKET,SO_REUSEADDR,(char *)&flag,sizeof(int))<0){//设置端口复用
			perror("setsockopt\n");
			exit(1);
		}
		if(setsockopt(sockfd,IPPROTO_TCP,TCP_NODELAY,(char*)&flag,sizeof(int))<0){//屏蔽Nagle算法，避免不可预测的延迟
			perror("setsockopt\n");
			exit(1);
		}
		printf("client connect %s succeed\n",ip);
		return sockfd;
	}
	return -1;
}

int server_accept(ushort port,int backlog){
	printf("server accept begin----\n");
//@	int listenfd;//服务器监听套接字
	int connfd;//和listenfd连接的客户端描述符
	int sockfd;//监听到的客户端文件描述符
	ssize_t nready,res;
//@	ssize_t efd;
	socklen_t clilen;
	struct sockaddr_in servaddr,cliaddr;
	struct epoll_event tep,ep[OPEN_MAX];
	char str[INET_ADDRSTRLEN];
	listenfd=socket(AF_INET,SOCK_STREAM,0);

	int opt=1;
	if(setsockopt(listenfd,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt))<0){//设置端口复用
		perror("setsocket\n");
		exit(1);
	}
	bzero(&servaddr,sizeof(servaddr));
	servaddr.sin_family=AF_INET;
	servaddr.sin_addr.s_addr=htonl(INADDR_ANY);
	servaddr.sin_port=htons(port);

	bind(listenfd,(struct sockaddr*)&servaddr,sizeof(servaddr));
	listen(listenfd,backlog);

	efd=epoll_create(OPEN_MAX);//创建epoll模型，efd指向红黑树根节点
	if(efd==-1)
	{
		perror("epoll_create error\n");
	}
	tep.events=EPOLLIN;//指定lfd的监听事件为“读”
	tep.data.fd=listenfd;
	res=epoll_ctl(efd,EPOLL_CTL_ADD,listenfd,&tep);//将lfd及对应的结构体设置到树上，efd可找到该树
	if(res==-1)
	{
		perror("epoll_ctl error\n");
	}
	for(;;){
	/*epoll为server阻塞监听事件，ep为struct epoll_event类型数组，OPEN_MAX为数组容量，-1表示永久阻塞*/
		nready=epoll_wait(efd,ep,OPEN_MAX,-1);
		if(nready==-1)
			perror("epoll_wait error");
		for(int i=0;i<nready;i++)
		{
			if(!(ep[i].events & EPOLLIN))//如果不是“读”事件，继续循环
				continue;
			if(ep[i].data.fd==listenfd)//判断满足事件的fd是不是lfd
			{
				clilen=sizeof(cliaddr);
				connfd=accept(listenfd,(struct sockaddr*)&cliaddr,&clilen);//接受连接，返回一个已连接的客户端的文件描述符
				printf("server received from %s\n",inet_ntop(AF_INET,&cliaddr.sin_addr,str,sizeof(str)));
			
				tep.events=EPOLLIN;
				tep.data.fd=connfd;
				res=epoll_ctl(efd,EPOLL_CTL_ADD,connfd,&tep);
				if(res==-1)
				{
					perror("epoll_ctl error\n");
				}
			}
			else{//不是lfd，就是客户端已连接的文件描述符了
				sockfd=ep[i].data.fd;
				return sockfd;
				////
				char *buf2=(char*)malloc(sizeof(Client2MetadataRequest));
				size_t needRecv=sizeof(Client2MetadataRequest);
				sockfd=ep[i].data.fd;
			//	memset(buf2,0,buf2);
				//read(sockfd,buf,MAX_LINE);
				recv_bytes(sockfd,buf2,needRecv);
				Client2MetadataRequest *r2=(Client2MetadataRequest*)malloc(sizeof(Client2MetadataRequest));
				memcpy(r2,buf2,needRecv);
				printf("r2->requestTYPE=%d,r2->blockID=%lu,r2->nodeID=%d\n",r2->requestTYPE,r2->blockID,r2->nodeID);
			//	printf("测试sockfd=%d,buf1=%s\n",sockfd,buf1);

				int n;
				n=sizeof(r2);
				printf("read from client %d bytes\n",n);
				if(n==0){//读到0，说明客户端关闭连接
					printf("我是错误的读，发送有问题");
					res=epoll_ctl(efd,EPOLL_CTL_DEL,sockfd,NULL);//将该文件描述符从红黑树摘除
					if(res==-1)
						perror("epoll_ctl error\n");
					close(sockfd);
				}
				else if(n<0){//出错
					perror("read n<0 error:\n");
					res=epoll_ctl(efd,EPOLL_CTL_DEL,sockfd,NULL);
					if(res<0)
						perror("epoll_ctl error\n");
					close(sockfd);
				}
				else{//实际读到了数据
					printf("实际读到了数据\n");
					printf("返回的sockfd=%d\n",sockfd);
					return sockfd;
					//write(STDOUT_FILENO,buf,n);
					//printf("\n");
					//printf("msg:write back to client:%s\n",buf);
					//ssize_t size_write;
					//if((size_write=write(sockfd,buf,n))<0)
					//{
					//	perror("write to client error");
				//	}
					//close(sockfd);
				}
			
			}

		}
	}
	//close(listenfd);//6.22晚加，无用
	//close(efd);//6.22晚加，无用
}
/**
 * [send_bytes description]       一次性发送文件
 * @param  sockfd [description]   套接字
 * @param  buf    [description]   缓冲区
 * @param  nbytes [description]   字节大小
 * @return        [description]   总的发送文件大小
 */
/*一次性发送文件，sockfd较小，buf较大,防止覆盖*/
ssize_t send_bytes(int sockfd,char *buf,size_t nbytes)
{
	//printf("send_bytes测试:sockfd=%d\n",sockfd);
	size_t total=0;
	int once;
	for(once=0;total<nbytes;total+=once)
	{
		once=send(sockfd,buf+total,nbytes-total,0);
	//	printf("send_bytes测试:once=%d\n",once);
		if(once<=0) break;
	}
	return total;
}

/**
 * [recv_bytes description]      一次性接收文件
 * @param  sockfd [description]
 * @param  buf    [description]
 * @param  nbytes [description]
 * @return        [description]
 */
ssize_t recv_bytes(int sockfd,char *buf,size_t nbytes){
    size_t total=0;
    int once;
    for(once=0;total<nbytes;total+=once){
        once=recv(sockfd,buf+total,nbytes-total,0);
        if(once<=0)break;
    }
//	printf("测试：total=%lu\n",total);
    return total;
}

//读取块到节点的信息---随机分布
void ReadNode2BlockFile(char *filename)
{
	FILE *fp=fopen(filename,"r");
	if(fp==NULL)
	{
		printf("Node2BlockFile is not exist\n");
		return ;
	}
	for(int i=0;i<stripeNum;i++)
		{
			for(int j=0;j<nodeInStripe;j++)
				{
					fscanf(fp,"%d,",&node2blockTest[i][j]);
					printf(" node2blockTest[%d][%d]=%d ",i,j,node2blockTest[i][j]);
				}
			printf("\n");
			fscanf(fp,"\n");
		}
}
//读取配置文件信息
node *read_config(const char *filename)
{
	printf("read_config begin-----\n");
	node *nodes=NULL;
	FILE *fp=fopen(filename,"r");
	      if(fp==NULL)
	      {
	          printf("%s is not exist\n",filename);
	          return nodes;
	     }
		 nodes=(node*)malloc(NodeNumTotal*sizeof(node));
		int Data_Parity_Nodes=6;
		char strbuf[512]={'\0'};
		int i;
		for(i=0;i<Data_Parity_Nodes &&!feof(fp);i++)//数据节点和校验节点
		{
			while(fgets(strbuf,sizeof(strbuf),fp) && strbuf[0]==COMMENTS);//读一行数据，且行不以#开头，读到strbuf里
			    	sscanf(strbuf,"%s",nodes[i].ip);
					nodes[i].nodeID=i;
		}
		for(i=Data_Parity_Nodes;i<NodeNumTotal-1&&!feof(fp);i++)//client
		        {
		               while(fgets(strbuf,sizeof(strbuf),fp) && strbuf[0]==COMMENTS);//读一行数据，且行不以#开头， 读到strbuf里
			                      sscanf(strbuf,"%s",nodes[i].ip);
			                      nodes[i].nodeID=i;
				    }
		/*元数据管理器节点*/			
		 while(fgets(strbuf,sizeof(strbuf),fp) && strbuf[0]==COMMENTS);
			      sscanf(strbuf,"%s",nodes[NodeNumTotal-1].ip);
				  nodes[NodeNumTotal-1].nodeID = NodeNumTotal-1;
		fclose(fp);
		/*读机器信息测试*/
		for(int i=0;i<NodeNumTotal;i++)
		     {
		         printf("nodes[%d].nodeID=%d,nodes[%d].ip=%s\n",i,nodes[i].nodeID,i,nodes[i].ip);
		     }
		/*读机器信息测试*/
	    return nodes;
}

void get_rand_str(char s[],int num)//得到指定大小的随机字符串
{
	 char *str = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz,./;\"'<>?";
	 int i,lstr;
	 char ss[2] = {0};
	 lstr = strlen(str);//计算字符串长度
	 srand((unsigned int)time((time_t *)NULL));//使用系统时间来初始化随机数发生器
	 for(i = 1; i <= num; i++){//按指定大小返回相应的字符串
		sprintf(ss,"%c",str[(rand()%lstr)]);//rand()%lstr 可随机返回0-71之间的整数, str[0-71]可随机得到其中的字符
	 strcat(s,ss);//将随机生成的字符串连接到指定数组后面
	}
}


#define ULEN 16 //IP长度
#define Block_Size 32*1024 //块大小
#define Client2MetadataPort 40000//@40000 //client与metadata通信端口
#define Client2MetadataPort2 40001//@client与metadata候选端口
#define Client2NodePort 41000 //client与datanode通信接口
#define Metadata2NodePort 43000 //datanode与metedata通信接口
#define Node2NodePort 45000 //datanode与datanode通信接口
#define ERR -1//错误标记
#define NodeNumTotal 9//节点总数
#include<arpa/inet.h>
#include<unistd.h>
#include<netdb.h>
#include<stdio.h>
#include<stdlib.h>
#include<time.h>
#include<errno.h>
#include<sys/epoll.h>
#include<string.h>
#include<ctype.h>
#include<netinet/tcp.h>//for TCP_NODELAY
#define OPEN_MAX 20//struct epoll_event类型数组ep的数组容量,OPEN_MAX为数组容量
#define MAX_LINE 8192//字符串缓冲区大小  
#define Max_Length_backlog 10 //socket可排队的最大连接个数
#define stripeNum 1000//1000个条带
#define nodeInStripe 6//4+2
int node2block[stripeNum][nodeInStripe];//块到节点的映射
int node2blockTest[stripeNum][nodeInStripe];
size_t efd;//红黑树 6.22晚加
int listenfd;//服务器监听套接字  6.22晚加

typedef struct _node{//节点信息
	int nodeID;    //节点编号
	char ip[ULEN];  //节点ip
}node;

typedef struct _block{//块信息
	long blockID;       //块号
	char blockBuf[Block_Size];//块内容
	int isParity;//是否是校验分块，初始化为-1；当等于0的时候，不是校验分块，当等于1的时候是校验分块
}Block;

typedef struct _BlockMeta{//数据块元数据信息
	long blockID;//块号
	int nodeID;//节点编号
}BlockMeta;
 
typedef struct _Client2MetadataRequest{//客户端、元数据通信
	int requestTYPE;//请求类型，初始化为-1，当等于0的时候，是读请求，当等于1的时候是写请求
	long blockID;//块号 块号除以n为条带数
	int nodeID;//节点号
}Client2MetadataRequest;

typedef struct _Client2MetadataResponse{
	long blockID;
	int nodeID;
}Client2MetadataResponse;//服务器返回给客户端的元信息

typedef struct _Client2NodeRequest{
	int requestTYPE;//请求类型
	long blockID;//块号
	int nodeID;//节点号
	char blockBuf[Block_Size];//块内容
}Client2NodeRequest;

int client_connect(char *ip,ushort port);//client尝试连接server
int server_accept(ushort port,int backlog);//server_ epoll bind listen accept 建立连接
ssize_t recv_bytes(int sockfd,char *buf,size_t nbytes);//一次性接收文件
ssize_t send_bytes(int sockfd,char *buf,size_t nbytes);////一次性发送文件

node *read_config(const char *filename);//读取配置文件信息
void ReadNode2BlockFile(char *filename);//读取块到节点的信息---随机分布
void get_rand_str(char s[],int num);//得到固定大小的随机字符串

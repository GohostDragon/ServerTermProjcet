#include <WS2tcpip.h>
#include <Windows.h>
#include <iostream>
#include <string>
#include <math.h>
#include <stdlib.h>
#include <time.h>
#include <atlImage.h>
#include <deque>

#include "resource.h"

#include "..\..\Protocol\2021_텀프_protocol.h"

using namespace std;
//서버 관련
#pragma comment(lib, "WS2_32.LIB")
//#pragma comment(linker, "/entry:WinMainCRTStartup /subsystem:console" ) 

constexpr auto BUF_SIZE = MAX_BUFFER;

// 보드맵 사이즈
#define BOARD_SHOW_SIZEX 16
#define BOARD_SHOW_SIZEY 16
#define BOARD_SIZEX 16
#define BOARD_SIZEY 16

// 최대 채팅 보이는 수
#define MAX_CHATLIST 5

// Server 관련
WSADATA WSAData;
SOCKET s_socket; // 서버 소켓
char serverip[32]; //서버아이피
WSAOVERLAPPED s_over;

typedef struct PlayerData
{
	int x = 3;
	int y = 3;
	bool bEnable = false;
	char o_type = 0;
};
PlayerData Players[MAX_USER];

PlayerData myPlayer;
int myid;


deque<string> chattxt;

// 윈도우 프로그래밍 관련
HINSTANCE g_hinst;
LPCTSTR lpszClass = "Window Class Name";
LRESULT CALLBACK WndProc(HWND hWnd, UINT iMessage, WPARAM wParam, LPARAM IParam);

//클라이언트 소켓 생성
void CreateClient(HWND hWnd);
//디버그 에러 출력
void display_error(const char* msg, int err_no);
//아이피 입력
BOOL CALLBACK DialogProc(HWND hDlg, UINT iMsg, WPARAM wParam, LPARAM lParam);
//Recv 스레드 함수
DWORD WINAPI RecvSendMsg(LPVOID arg);

void send_login_packet();
void send_move_packet(int dir);
void send_chat_packet(string txt);
void process_data(char* net_buf, size_t io_byte);
void ProcessPacket(char* ptr);

void putChatlist(const string& txt)
{
	if (chattxt.size() > MAX_CHATLIST)
	{
		chattxt.pop_back();
	}
	chattxt.push_front(txt);
}

BOOL CALLBACK DialogProc(HWND hDlg, UINT iMsg, WPARAM wParam, LPARAM lParam)
{
	switch (iMsg) {
	case WM_INITDIALOG:
		break;
	case WM_COMMAND:
		switch (LOWORD(wParam)) {
		case IDOK:
			GetDlgItemText(hDlg, IDC_IPADDRESS1, serverip, 32); // 스트링 복사
			EndDialog(hDlg, 0);
			break;
		case IDCANCEL:
			EndDialog(hDlg, 0);
			exit(0);
			break;
		}
		break;
	}
	return 0;
}

void display_error(const char* msg, int err_no)
{
	WCHAR* lpMsgBuf;
	FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM, NULL, err_no
		, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (LPTSTR)&lpMsgBuf, 0, NULL);
	cout << msg;
	wcout << L"에러 " << lpMsgBuf << endl;
	LocalFree(lpMsgBuf);
}

void CreateClient(HWND hWnd)
{
	wcout.imbue(locale("korean"));
	WSAStartup(MAKEWORD(2, 2), &WSAData);
	s_socket = WSASocketW(AF_INET, SOCK_STREAM, 0, 0, 0, WSA_FLAG_OVERLAPPED);
	SOCKADDR_IN svr_addr;
	memset(&svr_addr, 0, sizeof(svr_addr));
	svr_addr.sin_family = AF_INET;
	svr_addr.sin_port = htons(SERVER_PORT);
	DialogBox(g_hinst, MAKEINTRESOURCE(IDD_DIALOG1), hWnd, (DLGPROC)&DialogProc);
	inet_pton(AF_INET, serverip, &svr_addr.sin_addr);
	WSAConnect(s_socket, reinterpret_cast<sockaddr*>(&svr_addr), sizeof(svr_addr), NULL, NULL, NULL, NULL);

	send_login_packet();
	InvalidateRect(hWnd, NULL, FALSE);
}

DWORD WINAPI RecvSendMsg(LPVOID arg) 
{
	HWND hWnd = (HWND)arg;
	while (1) 
	{
		char net_buf[BUF_SIZE];

		WSABUF r_wsabuf[1];
		r_wsabuf[0].buf = (char*)&net_buf;
		r_wsabuf[0].len = BUF_SIZE;
		DWORD bytes_recv;
		DWORD r_flag = 0;
		auto recv_result = WSARecv(s_socket, r_wsabuf, 1, &bytes_recv, &r_flag, 0, 0);

		if (bytes_recv > 0) process_data(net_buf, bytes_recv);

		InvalidateRect(hWnd, NULL, FALSE);
	}
	return 0;
}

int WINAPI WinMain(HINSTANCE hinstance, HINSTANCE hPrevInstance, LPSTR IpszCmdParam, int nCmdShow)
{
	HWND hWnd;
	MSG Message;
	WNDCLASSEX WndClass;
	g_hinst = hinstance;
	//윈도우 클래스 구조체 값 설정
	WndClass.cbSize = sizeof(WndClass);
	WndClass.style = CS_HREDRAW | CS_VREDRAW;
	WndClass.lpfnWndProc = (WNDPROC)WndProc;
	WndClass.cbClsExtra = 0;
	WndClass.cbWndExtra = 0;
	WndClass.hInstance = hinstance;
	WndClass.hIcon = LoadIcon(NULL, IDI_APPLICATION);
	WndClass.hCursor = LoadCursor(NULL, IDC_ARROW);
	WndClass.hbrBackground = (HBRUSH)GetStockObject(WHITE_BRUSH);
	WndClass.lpszMenuName = NULL;
	//WndClass.lpszMenuName = MAKEINTRESOURCE(IDR_MENU1);
	WndClass.lpszClassName = lpszClass;
	WndClass.hIconSm = LoadIcon(NULL, IDI_APPLICATION);

	//윈도우 클래스 등록
	RegisterClassEx(&WndClass);
	//윈도우 생성
	hWnd = CreateWindow(lpszClass, "게임서버", WS_OVERLAPPEDWINDOW, 0, 0, 600, 623, NULL, (HMENU)NULL, hinstance, NULL);

	//윈도우 출력
	ShowWindow(hWnd, nCmdShow);
	UpdateWindow(hWnd);
	//클라이언트 소켓
	CreateClient(hWnd);
	CreateThread(NULL, 0, RecvSendMsg, (LPVOID)hWnd, 0, NULL); // Recv는 쓰레드를 돌려서 게속 받게 함
	//이벤트 루프 처리
	while (GetMessage(&Message, 0, 0, 0)) {
		TranslateMessage(&Message);
		DispatchMessage(&Message);
	}

	closesocket(s_socket);
	WSACleanup();
	return Message.wParam;
}


LRESULT CALLBACK WndProc(HWND hWnd, UINT iMessage, WPARAM wParam, LPARAM IParam)
{
	PAINTSTRUCT ps;
	static CImage background; //보드맵
	static CImage chess; //플레이어

	static RECT rectView; //클라이언트 창 크기
	static HDC hDC,memDC;

	static float dx, dy; //보드맵 한칸 크기
	static float mx, my; //마우스 좌표

	static bool chatmode = false;
	static string chatstr = "";

	switch (iMessage) {
	case WM_CREATE:
	{
		srand((unsigned)time(NULL));
		GetClientRect(hWnd, &rectView);

		dx = (float)rectView.right / BOARD_SIZEX, dy = (float)rectView.bottom / BOARD_SIZEY;

		background.Load("img\\chessboard.jpg");
		chess.Load("img\\chess.png");
		return 0;
	}
	case WM_PAINT:
	{
		CImage img;
		img.Create(rectView.right, rectView.bottom, 24);
		hDC = BeginPaint(hWnd, &ps);

		myPlayer.bEnable = true;

		memDC = img.GetDC();
		{
			Rectangle(memDC, 0, 0, rectView.right,rectView.bottom);
			//background.Draw(memDC, 0, 0, rectView.right, rectView.bottom);
			for (int x = 0; x < 250; x++)
			{
				for (int y = 0; y < 250; y++)
				{
					background.Draw(memDC
						, rectView.right / 2 * x - myPlayer.x * dx + BOARD_SIZEX / 2 * dx
						, rectView.bottom / 2 * y - myPlayer.y * dy + BOARD_SIZEY / 2 * dy
						, rectView.right / 2
						, rectView.bottom / 2);
				}
			}
			//chess.Draw(memDC, cx * dx, cy * dy, dx, dy);

			for (int i = 0; i < MAX_USER; i++)
			{
				if (Players[i].bEnable)
				{
					if (Players[i].o_type == 0)
					{
						chess.Draw(memDC
							, Players[i].x * dx - myPlayer.x * dx + BOARD_SIZEX / 2 * dx
							, Players[i].y * dy - myPlayer.y * dy + BOARD_SIZEY / 2 * dy
							, dx , dy
							, 132 * 3, 132
							, 132, 132);
						//chess2.Draw(memDC
						//	, Players[i].x * dx - myPlayer.x * dx + BOARD_SIZEX / 2 * dx
						//	, Players[i].y * dy - myPlayer.y * dy + BOARD_SIZEY / 2 * dy
						//	, dx, dy);
					}
					else if (Players[i].o_type == 1) {
						chess.Draw(memDC
							, Players[i].x * dx - myPlayer.x * dx + BOARD_SIZEX / 2 * dx
							, Players[i].y * dy - myPlayer.y * dy + BOARD_SIZEY / 2 * dy
							, dx , dy
							, 0, 132
							, 132, 132);
					}
				}
			}

			if (myPlayer.bEnable) {
				chess.Draw(memDC
					, BOARD_SIZEX / 2 * dx
					, BOARD_SIZEY / 2 * dy
					, dx , dy
					, 132 * 3, 0
					, 132, 132);

				//chess.Draw(memDC
				//	, BOARD_SIZEX / 2 * dx
				//	, BOARD_SIZEY / 2 * dy
				//	, dx, dy);
			}

			// 상단 UI
			RECT rt = { 510,10,600,50 };
			int a = 100;
			string str = "HP:" + to_string(a);
			DrawText(memDC, str.c_str(), -1, &rt, DT_VCENTER | DT_WORDBREAK);

			rt = { 510,30,600,100 };
			str = "MP:" + to_string(a);
			DrawText(memDC, str.c_str(), -1, &rt, DT_VCENTER | DT_WORDBREAK);


			// 채팅 UI
			if (chatmode)
			{
				rt = { 10, 530, 1000, 570 };
				string tmpchat = "채팅:" + chatstr;
				DrawText(memDC, tmpchat.c_str(), -1, &rt, DT_VCENTER | DT_WORDBREAK);
			}
			int i = 0;
			for (const auto& txt : chattxt) {
				rt = { 10,500-i*20,1000,540 - i * 20 };
				DrawText(memDC, txt.c_str(), -1, &rt, DT_VCENTER | DT_WORDBREAK);
				i++;
			}

			img.Draw(hDC, 0, 0);
			img.ReleaseDC();
		}
		EndPaint(hWnd, &ps);
		break;
	}
	case WM_KEYDOWN:
		if (chatmode == false) {
			int dir;
			if (wParam == VK_LEFT) {
				dir = 3;
				send_move_packet(dir);
			}
			else if (wParam == VK_RIGHT) {
				dir = 1;
				send_move_packet(dir);
			}
			else if (wParam == VK_UP) {
				dir = 0;
				send_move_packet(dir);
			}
			else if (wParam == VK_DOWN) {
				dir = 2;
				send_move_packet(dir);
			}
			else if (wParam == 't' || wParam == 'T') {
				chatmode = true;
			}
		} else {
			if (wParam == VK_RETURN) {
				if(chatstr.empty() == false)
					send_chat_packet(chatstr);
				//putChatlist(chattxt, chatstr);
				chatmode = false;
				chatstr = "";
			}
		}
		InvalidateRect(hWnd, NULL, FALSE);
		break;
	case WM_KEYUP:
		switch (wParam) {
		case VK_LEFT:
			break;
		case VK_RIGHT:
			break;
		case VK_UP:
			break;
		case VK_DOWN:
			break;
		}
		InvalidateRect(hWnd, NULL, FALSE);
		break;
	case WM_CHAR:
		if (chatmode)
		{
			if (wParam == VK_BACK)
			{
				if (chatstr.empty() == false)
					chatstr.pop_back();
			}else {
				chatstr.push_back(wParam);
			}
		}
		InvalidateRect(hWnd, NULL, FALSE);
		break;
	case WM_LBUTTONDOWN: // 버튼을 누르면 드래그 동작 시작
	{
		InvalidateRect(hWnd, NULL, FALSE);
		break;
	}
	case WM_LBUTTONUP: // 버튼을 놓으면 드래그 종료
		InvalidateRect(hWnd, NULL, FALSE);
		break;
	case WM_RBUTTONDOWN:
	{
		InvalidateRect(hWnd, NULL, FALSE);
		break;
	}
	case WM_MOUSEMOVE:
		break;

	case WM_TIMER: // 시간이 경과하면 메시지 자동 생성
		switch (wParam) {
		case 1:
			break;
		case 2:
			break;
		case 3:
			break;
		}
		InvalidateRect(hWnd, NULL, FALSE);
		break;
	case WM_COMMAND:
		InvalidateRect(hWnd, NULL, FALSE);
		break;
	case WM_DESTROY:
		KillTimer(hWnd, 1);
		PostQuitMessage(0);
		return 0;
	}
	return(DefWindowProcA(hWnd, iMessage, wParam, IParam));//위의 세 메세지 외의 나머지 메세지는 OS로
}

void ProcessPacket(char* ptr)
{
	static bool first_time = true;
	switch (ptr[1])
	{
	case SC_LOGIN_OK:
	{
		sc_packet_login_ok* packet = reinterpret_cast<sc_packet_login_ok*>(ptr);
		myid = packet->id;
		myPlayer.x = packet->x;
		myPlayer.y = packet->y;
		myPlayer.bEnable = true;
	}
	break;
	case SC_ADD_OBJECT:
	{
		sc_packet_add_object* my_packet = reinterpret_cast<sc_packet_add_object*>(ptr);
		int id = my_packet->id;

		if (id < MAX_USER) {
			Players[id].x = my_packet->x;
			Players[id].y = my_packet->y;
			Players[id].o_type = my_packet->obj_class;
			Players[id].bEnable = true;
		}
		break;
	}
	case SC_POSITION:
	{
		sc_packet_position* my_packet = reinterpret_cast<sc_packet_position*>(ptr);
		int other_id = my_packet->id;
		if (other_id == myid) {
			myPlayer.x = my_packet->x;
			myPlayer.y = my_packet->y;
		}
		else if (other_id < MAX_USER) {
			Players[other_id].x = my_packet->x;
			Players[other_id].y = my_packet->y;
		}
		else {
			//npc[other_id - NPC_START].x = my_packet->x;
			//npc[other_id - NPC_START].y = my_packet->y;
		}
		break;
	}

	case SC_REMOVE_OBJECT:
	{
		sc_packet_remove_object* my_packet = reinterpret_cast<sc_packet_remove_object*>(ptr);
		int other_id = my_packet->id;
		if (other_id == myid) {
			myPlayer.bEnable = false;
		}
		else if (other_id < MAX_USER) {
			Players[other_id].bEnable = false;
		}
		else {
			//		npc[other_id - NPC_START].attr &= ~BOB_ATTR_VISIBLE;
		}
		break;
	}
	case SC_CHAT:
	{
		sc_packet_chat* my_packet = reinterpret_cast<sc_packet_chat*>(ptr);
		//string txt = "[" + to_string(my_packet->id) + "]" + ": " + my_packet->message;
		putChatlist(my_packet->message);
		break;
	}
	default:
		printf("Unknown PACKET type [%d]\n", ptr[1]);
	}
}

void process_data(char* net_buf, size_t io_byte)
{
	char* ptr = net_buf;
	static size_t in_packet_size = 0;
	static size_t saved_packet_size = 0;
	static char packet_buffer[BUF_SIZE];

	while (0 != io_byte) {
		if (0 == in_packet_size) in_packet_size = ptr[0];
		if (io_byte + saved_packet_size >= in_packet_size) {
			memcpy(packet_buffer + saved_packet_size, ptr, in_packet_size - saved_packet_size);
			ProcessPacket(packet_buffer);
			ptr += in_packet_size - saved_packet_size;
			io_byte -= in_packet_size - saved_packet_size;
			in_packet_size = 0;
			saved_packet_size = 0;
		}
		else {
			memcpy(packet_buffer + saved_packet_size, ptr, io_byte);
			saved_packet_size += io_byte;
			io_byte = 0;
		}
	}
}

void send_move_packet(int dir)
{
	cs_packet_move packet;
	packet.size = sizeof(packet);
	packet.type = CS_MOVE;
	packet.direction = dir;

	WSABUF s_wsabuf[1];
	s_wsabuf[0].buf = (char*)&packet;
	s_wsabuf[0].len = sizeof(packet);
	DWORD sent_bytes;
	WSASend(s_socket, s_wsabuf, 1, &sent_bytes, 0, 0, 0);
}

void send_login_packet()
{
	cs_packet_login packet;
	packet.size = sizeof(packet);
	packet.type = CS_LOGIN;
	//strcpy_s(packet.name, to_string(rand() % 100).c_str());
	//strcpy_s(packet.name, "nickname");

	WSABUF s_wsabuf[1];
	s_wsabuf[0].buf = (char*)&packet;
	s_wsabuf[0].len = sizeof(packet);
	DWORD sent_bytes;
	WSASend(s_socket, s_wsabuf, 1, &sent_bytes, 0, 0, 0);
}

void send_chat_packet(string txt)
{
	cs_packet_chat packet;
	packet.size = sizeof(packet);
	packet.type = CS_CHAT;
	strcpy_s(packet.message, txt.c_str());

	WSABUF s_wsabuf[1];
	s_wsabuf[0].buf = (char*)&packet;
	s_wsabuf[0].len = sizeof(packet);
	DWORD sent_bytes;
	WSASend(s_socket, s_wsabuf, 1, &sent_bytes, 0, 0, 0);
}
#include <iostream>
#include <map>
#include <thread>
#include <vector>
#include <mutex>
#include <unordered_set>
#include <queue>
#include <WS2tcpip.h>
#include <MSWSock.h>

extern "C" {
#include "lua.h"
#include "lauxlib.h"
#include "lualib.h"
}

#pragma comment (lib, "lua54.lib")

#include "../../Protocol/2021_텀프_protocol.h"

#pragma comment(lib, "Ws2_32.lib")
#pragma comment(lib, "MSWSock.lib")

using namespace std;

constexpr int NUM_THREADS = 4;

enum OP_TYPE { OP_RECV, OP_SEND, OP_ACCEPT, OP_RANDOM_MOVE, OP_PLAYER_MOVE };
enum S_STATE {STATE_FREE, STATE_CONNECTED, STATE_INGAME};
struct EX_OVER {
	WSAOVERLAPPED	m_over;
	WSABUF			m_wsabuf[1];
	unsigned char	m_netbuf[MAX_BUFFER];
	OP_TYPE			m_op;
	SOCKET			m_csocket;
	int				m_traget_id;
};

struct SESSION
{
	int				m_id;
	EX_OVER			m_recv_over;
	unsigned char	m_prev_recv;
	SOCKET   m_s;

	S_STATE	m_state;	
	mutex	m_lock;
	char	m_name[MAX_ID_LEN];
	short	m_x, m_y;
	int		last_move_time;
	unordered_set <int> m_viewlist;
	mutex	m_vl;
	lua_State* L;
	mutex	m_sl;
};

struct timer_event {
	int object_id;
	OP_TYPE	event_type;
	chrono::system_clock::time_point exec_time;
	int target_id;

	constexpr bool operator < (const timer_event& l) const
	{
		return exec_time > l.exec_time;
	}
};

priority_queue <timer_event> timer_queue;
mutex timer_lock;

SESSION players[MAX_USER];
SOCKET listenSocket;
HANDLE h_iocp;

void display_error(const char* msg, int err_no)
{
	WCHAR* lpMsgBuf;
	FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM,
		NULL, err_no,
		MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
		(LPTSTR)&lpMsgBuf, 0, NULL);
	cout << msg;
	wcout << L" 에러 " << lpMsgBuf << endl;
	LocalFree(lpMsgBuf);
}

int add_event(int id, OP_TYPE ev, int delay_ms)
{
	using namespace chrono;
	timer_event event{ id, ev, system_clock::now() + milliseconds(delay_ms), 0 };	
	timer_lock.lock();
	timer_queue.push(event);
	timer_lock.unlock();
	return 1;
}

bool is_npc(int id)
{
	return id >= NPC_ID_START;
}

bool can_see(int id_a, int id_b)
{
	return VIEW_RADIUS * VIEW_RADIUS >= (players[id_a].m_x - players[id_b].m_x) 
		* (players[id_a].m_x - players[id_b].m_x)
		+ (players[id_a].m_y - players[id_b].m_y) 
		* (players[id_a].m_y - players[id_b].m_y);
}

void send_packet(int p_id, void* buf)
{
	EX_OVER* s_over = new EX_OVER;

	unsigned char packet_size = reinterpret_cast<unsigned char*>(buf)[0];
	s_over->m_op = OP_SEND;
	memset(&s_over->m_over, 0, sizeof(s_over->m_over));
	memcpy(s_over->m_netbuf, buf, packet_size);
	s_over->m_wsabuf[0].buf = reinterpret_cast<char*>(s_over->m_netbuf);
	s_over->m_wsabuf[0].len = packet_size;

	WSASend(players[p_id].m_s, s_over->m_wsabuf, 1, 0, 0, &s_over->m_over, 0);
}

void send_login_info(int p_id)
{
	sc_packet_login_ok packet;
	packet.HP = 100;
	packet.id = p_id;
	packet.LEVEL = 3;
	packet.size = sizeof(packet);
	packet.type = SC_LOGIN_OK;
	packet.x = players[p_id].m_x;
	packet.y = players[p_id].m_y;
	send_packet(p_id, &packet);
}

void send_move_packet(int c_id, int p_id)
{
	sc_packet_position packet;
	packet.id = p_id;
	packet.size = sizeof(packet);
	packet.type = SC_POSITION;
	packet.x = players[p_id].m_x;
	packet.y = players[p_id].m_y;
	packet.move_time = players[p_id].last_move_time;
	send_packet(c_id, &packet);
}

void send_pc_login(int c_id, int p_id)
{
	sc_packet_add_object packet;
	packet.id = p_id;
	packet.size = sizeof(packet);
	packet.type = SC_ADD_OBJECT;
	packet.x = players[p_id].m_x;
	packet.y = players[p_id].m_y;
	strcpy_s(packet.name, players[p_id].m_name);
	if (true == is_npc(p_id)) 
		packet.obj_class = 1;
	else 
		packet.obj_class = 0;
	send_packet(c_id, &packet);
}

void send_chat(int c_id, int p_id, const char *mess)
{
	sc_packet_chat packet;
	packet.id = p_id;
	packet.size = sizeof(packet);
	packet.type = SC_CHAT;
	strcpy_s(packet.message, mess);
	send_packet(c_id, &packet);
}

void send_pc_logout(int c_id, int p_id)
{
	sc_packet_remove_object packet;
	packet.id = p_id;
	packet.size = sizeof(packet);
	packet.type = SC_REMOVE_OBJECT;
	send_packet(c_id, &packet);
}

void player_move(int p_id, char dir)
{
	short x = players[p_id].m_x;
	short y = players[p_id].m_y;
	switch (dir) {
	case 0: if (y > 0) y--; break;
	case 1: if (x < (WORLD_WIDTH - 1)) x++; break;
	case 2: if (y < (WORLD_HEIGHT - 1)) y++; break;
	case 3: if (x > 0) x--; break;
	}
	players[p_id].m_x = x;
	players[p_id].m_y = y;

	players[p_id].m_vl.lock();
	unordered_set <int> old_vl = players[p_id].m_viewlist;
	players[p_id].m_vl.unlock();

	send_move_packet(p_id, p_id);

	unordered_set <int> new_vl;
	for (auto& cl : players) {
		if (p_id == cl.m_id) continue;
		cl.m_lock.lock();
		if (STATE_INGAME != cl.m_state) {
			cl.m_lock.unlock();
			continue;
		}
		if (can_see(p_id, cl.m_id))
			new_vl.insert(cl.m_id);
		cl.m_lock.unlock();
	}
	
	for (auto npc : new_vl) {
		if (false == is_npc(npc)) continue;
		EX_OVER* ex_over = new EX_OVER;
		ex_over->m_op = OP_PLAYER_MOVE;
		ex_over->m_traget_id = p_id;
		PostQueuedCompletionStatus(h_iocp, 1, npc, &ex_over->m_over);
	}

	for (auto pl : new_vl) {
		players[p_id].m_vl.lock();
		if (0 == players[p_id].m_viewlist.count(pl)) {
			// 1. 새로 시야에 들어오는 플레이어 처리
			players[p_id].m_viewlist.insert(pl);
			players[p_id].m_vl.unlock();
			send_pc_login(p_id, pl);
			if (true == is_npc(pl)) continue;
			players[pl].m_vl.lock();
			if (0 == players[pl].m_viewlist.count(p_id)) {
				players[pl].m_viewlist.insert(p_id);
				players[pl].m_vl.unlock();
				send_pc_login(pl, p_id);
			}
			else {
				players[pl].m_vl.unlock();
				send_move_packet(pl, p_id);
			}
		}
		else {
			// 2. 처음 부터 끝까지 시야에 존재하는 플레이어 처리
			players[p_id].m_vl.unlock();
			if (true == is_npc(p_id)) continue;
			players[pl].m_vl.lock();
			if (0 == players[pl].m_viewlist.count(p_id)) {
				players[pl].m_viewlist.insert(p_id);
				players[pl].m_vl.unlock();
				send_pc_login(pl, p_id);
			}
			else {
				players[pl].m_vl.unlock();
				send_move_packet(pl, p_id);
			}
		}
	}

	for (auto pl : old_vl) {
		if (0 == new_vl.count(pl)) {
		players[p_id].m_vl.lock();	
		players[p_id].m_viewlist.erase(pl);
		players[p_id].m_vl.unlock();
			send_pc_logout(p_id, pl);
			if (true == is_npc(pl)) continue;
			players[pl].m_vl.lock();
			if (0 != players[pl].m_viewlist.count(p_id)) {
				players[pl].m_viewlist.erase(p_id);
				players[pl].m_vl.unlock();
				send_pc_logout(pl, p_id);
			} else
				players[pl].m_vl.unlock();
		}
	}
	// 3. 시야에서 벗어나는 플레이어 처리


}

void process_packet(int p_id, unsigned char* packet)
{
	cs_packet_login* p = reinterpret_cast<cs_packet_login*>(packet);
	switch (p->type) {
	case CS_LOGIN:
		players[p_id].m_lock.lock();
		//strcpy_s(players[p_id].m_name, p->name);
		players[p_id].m_x = 0; // rand() % BOARD_WIDTH;
		players[p_id].m_y = 0; // rand() % BOARD_HEIGHT;
		send_login_info(p_id);
		players[p_id].m_state = STATE_INGAME;
		players[p_id].m_lock.unlock();

		for (auto& p : players) {
			if (p.m_id == p_id) continue;
			p.m_lock.lock();
			if (p.m_state != STATE_INGAME) {
				p.m_lock.unlock();
				continue;
			}
			if (can_see(p_id, p.m_id)) {
				players[p_id].m_vl.lock();
				players[p_id].m_viewlist.insert(p.m_id);
				players[p_id].m_vl.unlock();
				send_pc_login(p_id, p.m_id);
				if (false == is_npc(p.m_id)) {
					p.m_vl.lock();
					p.m_viewlist.insert(p_id);
					p.m_vl.unlock();
					send_pc_login(p.m_id, p_id);
				}
			}
			p.m_lock.unlock();
		}

		break;
	case CS_MOVE: {
		cs_packet_move* move_packet = reinterpret_cast<cs_packet_move*>(packet);
		//players[p_id].last_move_time = move_packet->move_time;
		player_move(p_id, move_packet->direction);
	}
		break;
	case CS_CHAT: {
		cs_packet_chat* chat_packet = reinterpret_cast<cs_packet_chat*>(packet);
		for (auto& cl : players) {
			if (STATE_INGAME != cl.m_state) {
				continue;
			}
			string txt = "[" + to_string(p_id) + "]" + ": " + chat_packet->message;
			cout << "[채팅]" << txt << endl;
			send_chat(cl.m_id, p_id, txt.c_str());
		}
		break;
	}
	default :
		cout << "Unknown Packet Type [" << p->type << "] Error\n";
		exit(-1);
	}
}

void do_recv(int p_id)
{
	SESSION& pl = players[p_id];
	EX_OVER& r_over = pl.m_recv_over;
	// r_over.m_op = OP_RECV;
	memset(&r_over.m_over, 0, sizeof(r_over.m_over));
	r_over.m_wsabuf[0].buf = reinterpret_cast<CHAR *>(r_over.m_netbuf) + pl.m_prev_recv;
	r_over.m_wsabuf[0].len = MAX_BUFFER - pl.m_prev_recv;
	DWORD r_flag = 0;
	WSARecv(pl.m_s, r_over.m_wsabuf, 1, 0, &r_flag, &r_over.m_over, 0);
}

int get_new_player_id()
{
	for (int i = 0; i < MAX_USER; ++i) {
		players[i].m_lock.lock();
		if (STATE_FREE == players[i].m_state) {
			players[i].m_state = STATE_CONNECTED;
			players[i].m_vl.lock();
			players[i].m_viewlist.clear();
			players[i].m_vl.unlock();
			players[i].m_lock.unlock();
			return i;
		}
		players[i].m_lock.unlock();
	}
	return -1;
}

void do_accept(SOCKET s_socket, EX_OVER *a_over)
{
	SOCKET c_socket = WSASocket(AF_INET, SOCK_STREAM, 0, NULL, 0, WSA_FLAG_OVERLAPPED);
	memset(&a_over->m_over, 0, sizeof(a_over->m_over));
	DWORD num_byte;
	int addr_size = sizeof(SOCKADDR_IN) + 16;
	a_over->m_csocket = c_socket;
	BOOL ret = AcceptEx(s_socket, c_socket, a_over->m_netbuf, 0, addr_size, addr_size, &num_byte, &a_over->m_over);
	if (FALSE == ret) {
		int err = WSAGetLastError();
		if (WSA_IO_PENDING != err) {
			display_error("AcceptEx : ", err);
			exit(-1);
		}
	}
}

void disconnect(int p_id)
{

	players[p_id].m_lock.lock();
	unordered_set <int> old_vl = players[p_id].m_viewlist;
	players[p_id].m_state = STATE_CONNECTED;
	closesocket(players[p_id].m_s);
	players[p_id].m_state = STATE_FREE;
	players[p_id].m_lock.unlock();
	// players.erase(p_id);
	for (auto &cl : old_vl) {
		if (true == is_npc(cl)) continue;
		players[cl].m_lock.lock();
		if (STATE_INGAME != players[cl].m_state) {
			players[cl].m_lock.unlock();
			continue;
		}
		send_pc_logout(players[cl].m_id, p_id);
		players[cl].m_lock.unlock();
	}
}

void worker()
{
	while (true) {
		DWORD num_byte;
		ULONG_PTR i_key;
		WSAOVERLAPPED* over;
		BOOL ret = GetQueuedCompletionStatus(h_iocp, &num_byte, &i_key, &over, INFINITE);
		int key = static_cast<int> (i_key);
		if (FALSE == ret) {
			int err = WSAGetLastError();
			display_error("GQCS : ", err);
			disconnect(key);
			continue;
		}
		EX_OVER* ex_over = reinterpret_cast<EX_OVER*>(over);
		switch (ex_over->m_op) {
		case OP_RECV:
		{
			unsigned char* ps = ex_over->m_netbuf;
			int remain_data = num_byte + players[key].m_prev_recv;
			while (remain_data > 0) {
				int packet_size = ps[0];
				if (packet_size > remain_data) break;
				process_packet(key, ps);
				remain_data -= packet_size;
				ps += packet_size;
			}
			if (remain_data > 0)
				memcpy(ex_over->m_netbuf, ps, remain_data);
			players[key].m_prev_recv = remain_data;
			do_recv(key);
		}
		break;
		case OP_SEND:
			if (num_byte != ex_over->m_wsabuf[0].len)
				disconnect(key);
			delete ex_over;
			break;
		case OP_ACCEPT:
		{
			SOCKET c_socket = ex_over->m_csocket;
			int p_id = get_new_player_id();
			if (-1 == p_id) {
				closesocket(c_socket);
				do_accept(listenSocket, ex_over);
				continue;
			}



			SESSION& n_s = players[p_id];
			n_s.m_lock.lock();
			n_s.m_state = STATE_CONNECTED;
			n_s.m_id = p_id;
			n_s.m_prev_recv = 0;
			n_s.m_recv_over.m_op = OP_RECV;
			n_s.m_s = c_socket;
			n_s.m_x = 3;
			n_s.m_y = 3;
			n_s.m_name[0] = 0;
			n_s.m_lock.unlock();

			CreateIoCompletionPort(reinterpret_cast<HANDLE>(c_socket), h_iocp, p_id, 0);

			do_recv(p_id);
			do_accept(listenSocket, ex_over);
			cout << "New CLient [" << p_id << "] connected.\n";
		}
		break;
		case OP_RANDOM_MOVE:
		{
			////
			delete ex_over;
		}
		break;
		case OP_PLAYER_MOVE:
		{
			players[key].m_sl.lock();
			lua_State* L = players[key].L;
			lua_getglobal(L, "event_player_move");
			lua_pushnumber(L, ex_over->m_traget_id);
			lua_pcall(L, 1, 0, 0);
			players[key].m_sl.unlock();
			delete ex_over;
		}
		break;
		default: cout << "Unknown GQCS Error!\n";
			exit(-1);
		}
	}
}

void do_random_move_npc(int id)
{

	unordered_set <int> old_vl;
	for (auto &pl : players) {
		if (true == is_npc(pl.m_id)) continue;
		if (STATE_INGAME != pl.m_state) continue;
		if (true == can_see(id, pl.m_id))
			old_vl.insert(pl.m_id);
	}

	int x = players[id].m_x;
	int y = players[id].m_y;
	switch (rand() % 4) {
	case 0: if (x>0) x--; break;
	case 1: if (x < (WORLD_WIDTH - 1)) x++; break;
	case 2: if (y>0) y--; break;
	case 3: if (y < (WORLD_HEIGHT -1)) y++; break;
	}
	players[id].m_x = x;
	players[id].m_y = y;

	unordered_set <int> new_vl;
	for (auto& pl : players) {
		if (true == is_npc(pl.m_id)) continue;
		if (STATE_INGAME != pl.m_state) continue;
		if (true == can_see(id, pl.m_id))
			new_vl.insert(pl.m_id);
	}

	for (auto pl : old_vl) {
		if (0 == new_vl.count(pl)) { // 시야에서 벗어남
			players[pl].m_vl.lock();
			players[pl].m_viewlist.erase(id);
			players[pl].m_vl.unlock();
			send_pc_logout(pl, id);
		}
		else {
			send_move_packet(pl, id);
		}
	}
	for (auto pl : new_vl) {
		if (0 == old_vl.count(pl)) {
			players[pl].m_vl.lock();
			players[pl].m_viewlist.insert(id);
			players[pl].m_vl.unlock();
			send_pc_login(pl, id);
		}
	}
}

void do_ai()
{
	while (true) {
		auto start_t = chrono::system_clock::now();
		for (int i = NPC_ID_START; i < MAX_USER; ++i) {
			bool ai_on = false;
			for (int pl = 0; pl < NPC_ID_START; ++pl) {
				if (players[pl].m_state != STATE_INGAME) continue;
				if (can_see(i, pl)) {
					ai_on = true;
					break;
				}
			}
			if (true == ai_on) 
				do_random_move_npc(i);
		}
		auto end_t = chrono::system_clock::now();
		auto duration = end_t - start_t;
		cout << "AI Exec Time : " <<
			chrono::duration_cast<chrono::milliseconds>(duration).count()
			<< "ms.\n";
		this_thread::sleep_for(start_t + chrono::seconds(1) - end_t);
	}
}

void do_timer()
{
	using namespace chrono;

	for (;;) {
		timer_lock.lock();
		if (timer_queue.top().exec_time > system_clock::now()) {
			timer_lock.unlock();
			this_thread::sleep_for(10ms);
		}
		else {
			auto ev = timer_queue.top();
			timer_queue.pop();
			timer_lock.unlock();
			switch (ev.event_type) {
			case OP_RANDOM_MOVE:
				EX_OVER* ex_over = new EX_OVER;
				ex_over->m_op = OP_RANDOM_MOVE;
				PostQueuedCompletionStatus(h_iocp, 1, ev.object_id, &ex_over->m_over);
				//do_random_move_npc(ev.object_id);
				//add_event(ev.object_id, OP_RANDOM_MOVE, 1000);
				break;
			}
		}
	}
}

int API_get_x(lua_State* L)
{
	int obj_id = lua_tonumber(L, -1);
	lua_pop(L, 2);
	int x = players[obj_id].m_x;
	lua_pushnumber(L, x);
	return 1;
}

int API_get_y(lua_State* L)
{
	int obj_id = lua_tonumber(L, -1);
	lua_pop(L, 2);
	int y = players[obj_id].m_y;
	lua_pushnumber(L, y);
	return 1;
}

int API_send_message(lua_State* L)
{
	int obj_id = lua_tonumber(L, -3);
	int p_id = lua_tonumber(L, -2);
	const char* mess = lua_tostring(L, -1);
	lua_pop(L, 4);
	send_chat(p_id, obj_id, mess);
	return 0;
}

int main()
{
	wcout.imbue(locale("korean"));

	for (int i = 0; i < MAX_USER;++i) {
		auto& pl = players[i];
		pl.m_id = i;
		pl.m_state = STATE_FREE;
		pl.last_move_time = 0;
		// pl.m_viewlist.clear();
		/*
		if (true == is_npc(i)) {
			//strcpy_s(pl.m_name, to_string(i).c_str());
			pl.m_x = rand() % WORLD_WIDTH;
			pl.m_y = rand() % WORLD_HEIGHT;
			pl.m_state = STATE_INGAME;
			add_event(i, OP_RANDOM_MOVE, 1000);
			pl.L = luaL_newstate();
			luaL_openlibs(pl.L);
			luaL_loadfile(pl.L, "monster_ai.lua");
			lua_pcall(pl.L, 0, 0, 0);

			lua_getglobal(pl.L, "set_o_id");
			lua_pushnumber(pl.L, i);
			lua_pcall(pl.L, 1, 0, 0);

			lua_register(pl.L, "API_send_message", API_send_message);
			lua_register(pl.L, "API_get_x", API_get_x);
			lua_register(pl.L, "API_get_y", API_get_y);
		}
		*/
	}

	WSADATA WSAData;
	WSAStartup(MAKEWORD(2, 2), &WSAData);
	listenSocket = WSASocket(AF_INET, SOCK_STREAM, 0, NULL, 0, WSA_FLAG_OVERLAPPED);
	SOCKADDR_IN serverAddr;
	memset(&serverAddr, 0, sizeof(SOCKADDR_IN));
	serverAddr.sin_family = AF_INET;
	serverAddr.sin_port = htons(SERVER_PORT);
	serverAddr.sin_addr.S_un.S_addr = htonl(INADDR_ANY);
	bind(listenSocket, (struct sockaddr*)&serverAddr, sizeof(SOCKADDR_IN));
	listen(listenSocket, SOMAXCONN);

	h_iocp = CreateIoCompletionPort(INVALID_HANDLE_VALUE, 0, 0, 0);
	CreateIoCompletionPort(reinterpret_cast<HANDLE>(listenSocket), 
		h_iocp, 100000, 0);

	EX_OVER a_over;
	a_over.m_op = OP_ACCEPT;
	do_accept(listenSocket, &a_over);

	vector <thread> worker_threads;
	for (int i = 0; i < NUM_THREADS; ++i)
		worker_threads.emplace_back(worker);
	//thread ai_thread{ do_ai };
	//ai_thread.join();

	//thread timer_thread{ do_timer };
	//timer_thread.join();
	
	for (auto& th : worker_threads) th.join();
	closesocket(listenSocket);
	WSACleanup();
}


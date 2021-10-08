#include <iostream>
#include <map>
#include <thread>
#include <vector>
#include <mutex>
#include <unordered_set>
#include <queue>
#include <WS2tcpip.h>
#include <MSWSock.h>
#include <sqlext.h>

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

enum OP_TYPE { OP_RECV, OP_SEND, OP_ACCEPT, OP_RANDOM_MOVE, OP_PLAYER_MOVE, OP_NPC_RESPAWN, OP_PLAYER_SAVE, OP_PLAYER_HEALING, OP_PLAYER_SKIL_COOL };
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
	int		m_hp, m_mp, m_level, m_exp;
	int		last_move_time;
	int		m_obj_class;

	bool	moving = false;

	unordered_set <int> m_viewlist;
	mutex	m_vl;
	lua_State* L;
	mutex	m_sl;

	unordered_set <int> m_partylist;
	mutex	m_pl;

	ITEM	m_item;
	mutex	m_il;

	char coolTime;
	mutex	m_skil_lock;
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

struct SECTOR
{
	unordered_set <int> m_sectionlist;
	mutex	m_lock;
};

priority_queue <timer_event> timer_queue;
mutex timer_lock;

SESSION players[MAX_USER];
SOCKET listenSocket;
HANDLE h_iocp;
SECTOR g_ObjectListSector[Row][Col];

void do_random_move_npc(int id);

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

void db_err_display(SQLHANDLE hHandle, SQLSMALLINT hType, RETCODE RetCode)
{
	SQLSMALLINT iRec = 0;
	SQLINTEGER iError;
	WCHAR wszMessage[1000];
	WCHAR wszState[SQL_SQLSTATE_SIZE + 1];
	if (RetCode == SQL_INVALID_HANDLE) {
		fwprintf(stderr, L"Invalid handle!\n");
		return;
	}
	while (SQLGetDiagRec(hType, hHandle, ++iRec, wszState, &iError, wszMessage,
		(SQLSMALLINT)(sizeof(wszMessage) / sizeof(WCHAR)), (SQLSMALLINT*)NULL) == SQL_SUCCESS) {
		// Hide data truncated..
		if (wcsncmp(wszState, L"01004", 5)) {
			fwprintf(stderr, L"[%5.5s] %s (%d)\n", wszState, wszMessage, iError);
		}
	}
}

void save_player(int p_id) {
	SQLHENV henv;
	SQLHDBC hdbc;
	SQLHSTMT hstmt = 0;
	SQLRETURN retcode;

	// Allocate environment handle  
	retcode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);

	// Set the ODBC version environment attribute  
	if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
		retcode = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER*)SQL_OV_ODBC3, 0);

		// Allocate connection handle  
		if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
			retcode = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);

			// Set login timeout to 5 seconds  
			if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
				SQLSetConnectAttr(hdbc, SQL_LOGIN_TIMEOUT, (SQLPOINTER)5, 0);

				// Connect to data source  
				retcode = SQLConnect(hdbc, (SQLWCHAR*)L"GSDB", SQL_NTS, (SQLWCHAR*)NULL, 0, NULL, 0);

				// Allocate statement handle  
				if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
					retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);

					string s(players[p_id].m_name);
					wstring temp(s.length(), L' ');
					copy(s.begin(), s.end(), temp.begin());
					wstring str = L"EXEC save_t_player " 
						+ temp
						+ L", " + to_wstring(players[p_id].m_x)
						+ L", " + to_wstring(players[p_id].m_y)
						+ L", " + to_wstring(players[p_id].m_level)
						+ L", " + to_wstring(players[p_id].m_exp)
						+ L", " + to_wstring(players[p_id].m_hp)
						+ L", " + to_wstring(players[p_id].m_mp);
					retcode = SQLExecDirect(hstmt, (SQLWCHAR*)str.c_str(), SQL_NTS);
					if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
						cout << "[시스템]" << players[p_id].m_name << "님의 데이터 저장완료 되었습니다." << endl;
					}
					else {
						db_err_display(hstmt, SQL_HANDLE_STMT, retcode);
					}

					// Process data  
					if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
						SQLCancel(hstmt);
						SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
					}

					SQLDisconnect(hdbc);
				}

				SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
			}
		}
		SQLFreeHandle(SQL_HANDLE_ENV, henv);
	}
}

void load_player(int p_id) {
	SQLHENV henv;
	SQLHDBC hdbc;
	SQLHSTMT hstmt = 0;
	SQLRETURN retcode;
	SQLWCHAR szName[15];
	SQLINTEGER CharX, CharY, CharExp, CharLevel, CharHp, CharMp;
	SQLLEN cbName = 0, cbCharX = 0, cbCharY = 0, cbCharExp = 0, cbCharLevel = 0, cbCharHp = 0, cbCharMp = 0;

	// Allocate environment handle  
	retcode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);

	// Set the ODBC version environment attribute  
	if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
		retcode = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER*)SQL_OV_ODBC3, 0);

		// Allocate connection handle  
		if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
			retcode = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);

			// Set login timeout to 5 seconds  
			if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
				SQLSetConnectAttr(hdbc, SQL_LOGIN_TIMEOUT, (SQLPOINTER)5, 0);

				// Connect to data source  
				retcode = SQLConnect(hdbc, (SQLWCHAR*)L"GSDB", SQL_NTS, (SQLWCHAR*)NULL, 0, NULL, 0);

				// Allocate statement handle  
				if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
					retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);

					wstring str = L"EXEC load_t_player " + wstring(players[p_id].m_name, &players[p_id].m_name[MAX_ID_LEN]);
					retcode = SQLExecDirect(hstmt, (SQLWCHAR*)str.c_str(), SQL_NTS);
					if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
						// Bind columns 1, 2, and 3  
						retcode = SQLBindCol(hstmt, 1, SQL_C_ULONG, &CharX, 10, &cbCharX);
						retcode = SQLBindCol(hstmt, 2, SQL_C_ULONG, &CharY, 10, &cbCharY);
						retcode = SQLBindCol(hstmt, 3, SQL_C_ULONG, &CharLevel, 10, &cbCharLevel);
						retcode = SQLBindCol(hstmt, 4, SQL_C_ULONG, &CharExp, 10, &cbCharExp);
						retcode = SQLBindCol(hstmt, 5, SQL_C_ULONG, &CharHp, 10, &cbCharHp);
						retcode = SQLBindCol(hstmt, 6, SQL_C_ULONG, &CharMp, 10, &cbCharMp);

						// Fetch and print each row of data. On an error, display a message and exit.  
						for (int i = 0; ; i++) {
							retcode = SQLFetch(hstmt);
							if (retcode == SQL_ERROR || retcode == SQL_SUCCESS_WITH_INFO) {
								//db_err_display(hstmt, SQL_HANDLE_STMT, retcode);
							}
							if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO)
							{
								players[p_id].m_lock.lock();
								players[p_id].m_x = CharX;
								players[p_id].m_y = CharY;
								players[p_id].m_level = CharLevel;
								players[p_id].m_exp = CharExp;
								players[p_id].m_hp = CharHp;
								players[p_id].m_mp = CharMp;
								players[p_id].m_lock.unlock();
							}
							else
								break;
						}
					}
					else {
						db_err_display(hstmt, SQL_HANDLE_STMT, retcode);
					}

					// Process data  
					if (retcode == SQL_SUCCESS || retcode == SQL_SUCCESS_WITH_INFO) {
						SQLCancel(hstmt);
						SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
					}

					SQLDisconnect(hdbc);
				}

				SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
			}
		}
		SQLFreeHandle(SQL_HANDLE_ENV, henv);
	}
}

unordered_set <int> get_sector(int p_id, unordered_set <int>& old_vl, short x, short y)
{
	players[p_id].m_vl.lock();
	old_vl = players[p_id].m_viewlist;
	players[p_id].m_vl.unlock();

	// 섹터 영역 구하기
	int row = players[p_id].m_x / (VIEW_RADIUS * 2);
	int col = players[p_id].m_y / (VIEW_RADIUS * 2);

	players[p_id].m_x = x;
	players[p_id].m_y = y;

	// 움직여서 섹터 영역이 바뀜
	if (row != players[p_id].m_x / (VIEW_RADIUS * 2) || col != players[p_id].m_y / (VIEW_RADIUS * 2))
	{
		// 전에 있던 섹터 영역 플레이어 삭제
		g_ObjectListSector[row][col].m_lock.lock();
		g_ObjectListSector[row][col].m_sectionlist.erase(p_id);
		g_ObjectListSector[row][col].m_lock.unlock();

		row = players[p_id].m_x / (VIEW_RADIUS * 2);
		col = players[p_id].m_y / (VIEW_RADIUS * 2);

		// 새로 들어온 섹터 영역 플레이어 추가
		g_ObjectListSector[row][col].m_lock.lock();
		g_ObjectListSector[row][col].m_sectionlist.insert(p_id);
		g_ObjectListSector[row][col].m_lock.unlock();
	}

	g_ObjectListSector[row][col].m_lock.lock();
	unordered_set <int> tmpObjectListSector = g_ObjectListSector[row][col].m_sectionlist;
	g_ObjectListSector[row][col].m_lock.unlock();


	int trol = row + 1;
	int tcol = col;
	if ((players[p_id].m_x + 5) / (VIEW_RADIUS * 2) != row && (players[p_id].m_x + 5) < WORLD_WIDTH)
	{
		g_ObjectListSector[trol][tcol].m_lock.lock();
		unordered_set <int> tmpObjectListSector2 = g_ObjectListSector[trol][tcol].m_sectionlist;
		g_ObjectListSector[trol][tcol].m_lock.unlock();
		tmpObjectListSector.merge(tmpObjectListSector2);
	}

	trol = row - 1;
	tcol = col;
	if ((players[p_id].m_x - 5) / (VIEW_RADIUS * 2) != row && (players[p_id].m_x - 5) > 0)
	{
		g_ObjectListSector[trol][tcol].m_lock.lock();
		unordered_set <int> tmpObjectListSector2 = g_ObjectListSector[trol][tcol].m_sectionlist;
		g_ObjectListSector[trol][tcol].m_lock.unlock();
		tmpObjectListSector.merge(tmpObjectListSector2);
	}

	trol = row;
	tcol = col + 1;
	if ((players[p_id].m_y + 5) / (VIEW_RADIUS * 2) != col && (players[p_id].m_y + 5) < WORLD_HEIGHT)
	{
		g_ObjectListSector[trol][tcol].m_lock.lock();
		unordered_set <int> tmpObjectListSector2 = g_ObjectListSector[trol][tcol].m_sectionlist;
		g_ObjectListSector[trol][tcol].m_lock.unlock();
		tmpObjectListSector.merge(tmpObjectListSector2);
	}

	trol = row;
	tcol = col - 1;
	if ((players[p_id].m_y - 5) / (VIEW_RADIUS * 2) != col && (players[p_id].m_y - 5) > 0)
	{
		g_ObjectListSector[trol][tcol].m_lock.lock();
		unordered_set <int> tmpObjectListSector2 = g_ObjectListSector[trol][tcol].m_sectionlist;
		g_ObjectListSector[trol][tcol].m_lock.unlock();
		tmpObjectListSector.merge(tmpObjectListSector2);
	}

	trol = row + 1;
	tcol = col + 1;
	if ((players[p_id].m_x + 5) / (VIEW_RADIUS * 2) != row && (players[p_id].m_y + 5) / (VIEW_RADIUS * 2) != col
		&& (players[p_id].m_x + 5) < WORLD_WIDTH && (players[p_id].m_y + 5) < WORLD_HEIGHT)
	{
		g_ObjectListSector[trol][tcol].m_lock.lock();
		unordered_set <int> tmpObjectListSector2 = g_ObjectListSector[trol][tcol].m_sectionlist;
		g_ObjectListSector[trol][tcol].m_lock.unlock();
		tmpObjectListSector.merge(tmpObjectListSector2);
	}

	trol = row + 1;
	tcol = col - 1;
	if ((players[p_id].m_x + 5) / (VIEW_RADIUS * 2) != row && (players[p_id].m_y - 5) / (VIEW_RADIUS * 2) != col
		&& (players[p_id].m_x + 5) < WORLD_WIDTH && (players[p_id].m_y - 5) > 0)
	{
		g_ObjectListSector[trol][tcol].m_lock.lock();
		unordered_set <int> tmpObjectListSector2 = g_ObjectListSector[trol][tcol].m_sectionlist;
		g_ObjectListSector[trol][tcol].m_lock.unlock();
		tmpObjectListSector.merge(tmpObjectListSector2);
	}

	trol = row - 1;
	tcol = col + 1;
	if ((players[p_id].m_x - 5) / (VIEW_RADIUS * 2) != row && (players[p_id].m_y + 5) / (VIEW_RADIUS * 2) != col
		&& (players[p_id].m_x - 5) > 0 && (players[p_id].m_y + 5) < WORLD_HEIGHT)
	{
		g_ObjectListSector[trol][tcol].m_lock.lock();
		unordered_set <int> tmpObjectListSector2 = g_ObjectListSector[trol][tcol].m_sectionlist;
		g_ObjectListSector[trol][tcol].m_lock.unlock();
		tmpObjectListSector.merge(tmpObjectListSector2);
	}

	trol = row - 1;
	tcol = col - 1;
	if ((players[p_id].m_x - 5) / (VIEW_RADIUS * 2) != row && (players[p_id].m_y - 5) / (VIEW_RADIUS * 2) != col
		&& (players[p_id].m_x - 5) > 0 && (players[p_id].m_y - 5) > 0)
	{
		g_ObjectListSector[trol][tcol].m_lock.lock();
		unordered_set <int> tmpObjectListSector2 = g_ObjectListSector[trol][tcol].m_sectionlist;
		g_ObjectListSector[trol][tcol].m_lock.unlock();
		tmpObjectListSector.merge(tmpObjectListSector2);
	}
	return tmpObjectListSector;
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

bool can_buf(int id_a, int id_b)
{
	return 1 >= (players[id_a].m_x - players[id_b].m_x)
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
	packet.HP = players[p_id].m_hp;
	packet.MP = players[p_id].m_mp;
	packet.id = p_id;
	packet.LEVEL = players[p_id].m_level;
	packet.EXP = players[p_id].m_exp;
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

void send_add_object(int c_id, int p_id)
{
	sc_packet_add_object packet;
	packet.id = p_id;
	packet.size = sizeof(packet);
	packet.type = SC_ADD_OBJECT;
	packet.x = players[p_id].m_x;
	packet.y = players[p_id].m_y;
	packet.LEVEL = players[p_id].m_level;
	packet.EXP = players[p_id].m_exp;
	packet.HP = players[p_id].m_hp;
	packet.obj_class = players[p_id].m_obj_class;
	strcpy_s(packet.name, players[p_id].m_name);
	send_packet(c_id, &packet);
}

void send_login_fail(int p_id)
{
	sc_packet_login_fail packet;
	packet.size = sizeof(packet);
	packet.type = SC_LOGIN_FAIL;
	send_packet(p_id, &packet);
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

void send_stat_change(int c_id, int p_id)
{
	sc_packet_stat_change packet;
	packet.id = p_id;
	packet.size = sizeof(packet);
	packet.LEVEL = players[p_id].m_level;
	packet.EXP = players[p_id].m_exp;
	packet.HP = players[p_id].m_hp;
	packet.MP = players[p_id].m_mp;
	packet.STATE = players[p_id].m_state;
	packet.type = SC_STAT_CHANGE;
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

void send_party_invite(int c_id, int p_id)
{
	sc_packet_party_invite packet;
	packet.id = p_id;
	packet.size = sizeof(packet);
	packet.type = SC_PARTY_INVITE;
	send_packet(c_id, &packet);
}

void send_party_join(int c_id, int p_id)
{
	sc_packet_party_join packet;
	packet.id = p_id;
	packet.size = sizeof(packet);
	packet.type = SC_PARTY_JOIN;
	send_packet(c_id, &packet);
}

void send_party_leave(int c_id, int p_id)
{
	sc_packet_party_leave packet;
	packet.id = p_id;
	packet.size = sizeof(packet);
	packet.type = SC_PARTY_LEAVE;
	send_packet(c_id, &packet);
}

void send_item_change(int c_id, ITEM item)
{
	sc_packet_item_change packet;
	packet.size = sizeof(packet);
	packet.type = SC_ITEM_CHANGE;
	packet.item = item;
	send_packet(c_id, &packet);
}

void player_give_party_stat_info(int p_id, unordered_set<int> pl)
{
	for (auto& p : pl)
		send_stat_change(p, p_id);
}

void player_give_exp(int p_id, int monster_level)
{
	players[p_id].m_lock.lock();
	players[p_id].m_exp += monster_level * monster_level * 2;
	string txt = "[경험치] " + to_string(monster_level * monster_level * 2) + "경험치를 얻었다.";
	send_chat(p_id, p_id, txt.c_str());
	if (players[p_id].m_exp >= players[p_id].m_level * 200) {
		players[p_id].m_exp = 0;
		players[p_id].m_level += 1;
		players[p_id].m_lock.unlock();
	}
	else
	{
		players[p_id].m_lock.unlock();
	}
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
	if (players[p_id].m_x == x && players[p_id].m_y == y)
		return;
	unordered_set <int> old_vl;
	unordered_set <int> tmpObjectListSector = get_sector(p_id, old_vl, x, y);

	unordered_set <int> new_vl;
	for (auto& cl : tmpObjectListSector) {
		if (p_id == cl) continue;

		players[cl].m_lock.lock();
		if (STATE_INGAME != players[cl].m_state) {
			players[cl].m_lock.unlock();
			continue;
		}
		if (can_see(p_id, cl))
			new_vl.insert(cl);
		players[cl].m_lock.unlock();
	}

	send_move_packet(p_id, p_id);
	
	for (auto npc : new_vl) {
		if (players[npc].m_state != STATE_INGAME) continue;
		if (players[npc].m_obj_class < OB_MONSTER) continue;
		EX_OVER* ex_over = new EX_OVER;
		ex_over->m_op = OP_PLAYER_MOVE;
		ex_over->m_traget_id = p_id;
		PostQueuedCompletionStatus(h_iocp, 1, npc, &ex_over->m_over);
		if(players[npc].m_obj_class == OB_ONI)
			do_random_move_npc(npc);
	}

	for (auto pl : new_vl) {
		players[p_id].m_vl.lock();
		if (0 == players[p_id].m_viewlist.count(pl)) {
			// 1. 새로 시야에 들어오는 플레이어 처리
			players[p_id].m_viewlist.insert(pl);
			players[p_id].m_vl.unlock();
			send_add_object(p_id, pl);
			if (true == is_npc(pl)) continue;
			players[pl].m_vl.lock();
			if (0 == players[pl].m_viewlist.count(p_id)) {
				players[pl].m_viewlist.insert(p_id);
				players[pl].m_vl.unlock();
				send_add_object(pl, p_id);
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
				send_add_object(pl, p_id);
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

void player_teleport(int p_id, short x, short y)
{
	if (players[p_id].m_x == x && players[p_id].m_y == y)
		return;

	unordered_set <int> old_vl;
	unordered_set <int> tmpObjectListSector = get_sector(p_id, old_vl, x, y);

	unordered_set <int> new_vl;
	for (auto& cl : tmpObjectListSector) {
		if (p_id == cl) continue;

		players[cl].m_lock.lock();
		if (STATE_INGAME != players[cl].m_state) {
			players[cl].m_lock.unlock();
			continue;
		}
		if (can_see(p_id, cl))
			new_vl.insert(cl);
		players[cl].m_lock.unlock();
	}

	send_move_packet(p_id, p_id);

	for (auto npc : new_vl) {
		if (players[npc].m_state != STATE_INGAME) continue;
		if (players[npc].m_obj_class < OB_MONSTER) continue;
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
			send_add_object(p_id, pl);
			if (true == is_npc(pl)) continue;
			players[pl].m_vl.lock();
			if (0 == players[pl].m_viewlist.count(p_id)) {
				players[pl].m_viewlist.insert(p_id);
				players[pl].m_vl.unlock();
				send_add_object(pl, p_id);
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
				send_add_object(pl, p_id);
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
			}
			else
				players[pl].m_vl.unlock();
		}
	}
	// 3. 시야에서 벗어나는 플레이어 처리
}

void player_attack(int p_id)
{
	players[p_id].m_lock.lock();
	short px = players[p_id].m_x;
	short py = players[p_id].m_y;
	players[p_id].m_lock.unlock();

	players[p_id].m_vl.lock();
	unordered_set <int> old_vl = players[p_id].m_viewlist;
	players[p_id].m_vl.unlock();

	for (auto& npc : old_vl) {
		if (players[npc].m_state != STATE_INGAME) continue;
		if (players[npc].m_obj_class < OB_MONSTER) continue;
		players[npc].m_lock.lock();
		short nx = players[npc].m_x;
		short ny = players[npc].m_y;
		players[npc].m_lock.unlock();
		if ((nx == px && ny == py)
			|| (nx == px + 1 && ny == py)
			|| (nx == px - 1 && ny == py)
			|| (nx == px && ny == py + 1)
			|| (nx == px && ny == py - 1))
		{
			string txt = "[전투]" + string(players[npc].m_name) + "에게 데미지 30을 주었다!";
			send_chat(p_id, p_id, txt.c_str());

			players[npc].m_lock.lock();
			players[npc].m_hp -= 30;
			if (players[npc].m_hp <= 0) {
				players[npc].m_state = STATE_FREE;
				int monster_level = players[npc].m_level;
				players[npc].m_lock.unlock();
				string txt = "[전투]" + string(players[npc].m_name) + "를 쓰러뜨렸다!";
				send_chat(p_id, p_id, txt.c_str());
				add_event(npc, OP_NPC_RESPAWN, 30000);

				players[p_id].m_pl.lock();
				unordered_set<int> pl = players[p_id].m_partylist;
				players[p_id].m_pl.unlock();

				player_give_exp(p_id, monster_level);
				player_give_party_stat_info(p_id, pl);

				for (auto& p : pl)
				{
					player_give_exp(p, monster_level);
					player_give_party_stat_info(p, pl);
				}

				if (players[npc].m_obj_class == OB_GOBBLINE) {
					int r = rand() % 2;
					switch (r)
					{
					case 0:
					{
						players[p_id].m_il.lock();
						players[p_id].m_item.gobline_horn += 1;
						players[p_id].m_il.unlock();
						send_item_change(p_id, players[p_id].m_item);
						string txt = "[아이템] 유령의 천을 1개 얻었다.";
						send_chat(p_id, p_id, txt.c_str());
						break;
					}
					default:
						break;
					}
				} else 	if (players[npc].m_obj_class == OB_ONI) {
					int r = rand() % 2;
					switch (r)
					{
					case 0:
					{
						players[p_id].m_il.lock();
						players[p_id].m_item.oni_bet += 1;
						players[p_id].m_il.unlock();
						send_item_change(p_id, players[p_id].m_item);
						string txt = "[아이템] 사신 낫을 1개 얻었다.";
						send_chat(p_id, p_id, txt.c_str());
						break;
					}
					default:
						break;
					}
				} else if (players[npc].m_obj_class == OB_GHOST) {
					int r = rand() % 2;
					switch (r)
					{
					case 0:
					{
						players[p_id].m_il.lock();
						players[p_id].m_item.hp_postion += 1;
						players[p_id].m_il.unlock();
						send_item_change(p_id, players[p_id].m_item);
						string txt = "[아이템] hp 포션을 1개 얻었다.";
						send_chat(p_id, p_id, txt.c_str());
						break;
					}
					default:
						break;
					}
				}

			}
			else {
				players[npc].m_lock.unlock();
			}
			send_stat_change(p_id, npc);
		}
	}
	send_stat_change(p_id, p_id);
}

void party_leave(int p_id)
{
	players[p_id].m_pl.lock();
	unordered_set <int> pl = players[p_id].m_partylist;
	players[p_id].m_pl.unlock();
	for (auto& p : pl) {
		players[p].m_pl.lock();
		players[p].m_partylist.erase(p_id);
		players[p].m_pl.unlock();
		send_party_leave(p, p_id);
	}
}

void process_packet(int p_id, unsigned char* packet)
{
	cs_packet_login* p = reinterpret_cast<cs_packet_login*>(packet);
	switch (p->type) {
	case CS_LOGIN: {
		string temp = string(p->player_id);
		// 플레이어하는 사람 중에 동일한 사람이 있나?
		for (int p = 0; p < NPC_ID_START; ++p) {
			players[p].m_lock.lock();
			if (players[p].m_state != STATE_INGAME) {
				players[p].m_lock.unlock();
				continue;
			}
			if (strcmp(temp.c_str(), players[p].m_name) == 0) {
				players[p].m_lock.unlock();
				send_login_fail(p_id);
				return;
			}
			players[p].m_lock.unlock();
		}
		//for (auto& p : players) {
		//	if (p.m_state != STATE_INGAME) {
		//		players[p_id].m_lock.unlock();
		//		continue;
		//	}
		//	if (strcmp(players[p_id].m_name, p.m_name) == 0) {
		//		send_login_fail(p_id);
		//		players[p_id].m_lock.unlock();
		//		return;
		//	}
		//}

		//players[p_id].m_x = 0;
		//players[p_id].m_y = 0;
		players[p_id].m_lock.lock();
		strcpy_s(players[p_id].m_name, p->player_id);
		players[p_id].m_x = rand() % WORLD_WIDTH;
		players[p_id].m_y = rand() % WORLD_HEIGHT;
		players[p_id].m_hp = 100;
		players[p_id].m_mp = 100;
		players[p_id].m_exp = 0;
		players[p_id].m_level = 1;
		// DB안에 저장된 아이디가 있나?
		players[p_id].m_lock.unlock();

		players[p_id].m_skil_lock.lock();
		players[p_id].coolTime = 0;
		players[p_id].m_skil_lock.unlock();

		players[p_id].m_il.lock();
		players[p_id].m_item.hp_postion = 5;
		players[p_id].m_item.mp_postion = 5;
		players[p_id].m_item.gobline_horn = 0;
		players[p_id].m_item.oni_bet = 0;
		players[p_id].m_item.powerup = 0;
		players[p_id].m_il.unlock();
		load_player(p_id);

		cout << "[접속]" << players[p_id].m_name << "님께서 접속하셨습니다." << endl;

		add_event(p_id, OP_PLAYER_HEALING, 5000);
		add_event(p_id, OP_PLAYER_SAVE, 60000);

		players[p_id].m_lock.lock();
		send_login_info(p_id);
		players[p_id].m_state = STATE_INGAME;
		players[p_id].m_lock.unlock();
		send_item_change(p_id, players[p_id].m_item);
		// 섹터 영역 구하기
		int row = players[p_id].m_x / (VIEW_RADIUS * 2);
		int col = players[p_id].m_y / (VIEW_RADIUS * 2);

		g_ObjectListSector[row][col].m_lock.lock();
		g_ObjectListSector[row][col].m_sectionlist.insert(p_id);
		g_ObjectListSector[row][col].m_lock.unlock();

		g_ObjectListSector[row][col].m_lock.lock();
		unordered_set <int> tmpObjectListSector = g_ObjectListSector[row][col].m_sectionlist;
		g_ObjectListSector[row][col].m_lock.unlock();


		int trol = row + 1;
		int tcol = col;
		if ((players[p_id].m_x + 5) / (VIEW_RADIUS * 2) != row && (players[p_id].m_x + 5) < WORLD_WIDTH)
		{
			g_ObjectListSector[trol][tcol].m_lock.lock();
			unordered_set <int> tmpObjectListSector2 = g_ObjectListSector[trol][tcol].m_sectionlist;
			g_ObjectListSector[trol][tcol].m_lock.unlock();
			tmpObjectListSector.merge(tmpObjectListSector2);
		}

		trol = row - 1;
		tcol = col;
		if ((players[p_id].m_x - 5) / (VIEW_RADIUS * 2) != row && (players[p_id].m_x - 5) > 0)
		{
			g_ObjectListSector[trol][tcol].m_lock.lock();
			unordered_set <int> tmpObjectListSector2 = g_ObjectListSector[trol][tcol].m_sectionlist;
			g_ObjectListSector[trol][tcol].m_lock.unlock();
			tmpObjectListSector.merge(tmpObjectListSector2);
		}

		trol = row;
		tcol = col + 1;
		if ((players[p_id].m_y + 5) / (VIEW_RADIUS * 2) != col && (players[p_id].m_y + 5) < WORLD_HEIGHT)
		{
			g_ObjectListSector[trol][tcol].m_lock.lock();
			unordered_set <int> tmpObjectListSector2 = g_ObjectListSector[trol][tcol].m_sectionlist;
			g_ObjectListSector[trol][tcol].m_lock.unlock();
			tmpObjectListSector.merge(tmpObjectListSector2);
		}

		trol = row;
		tcol = col - 1;
		if ((players[p_id].m_y - 5) / (VIEW_RADIUS * 2) != col && (players[p_id].m_y - 5) > 0)
		{
			g_ObjectListSector[trol][tcol].m_lock.lock();
			unordered_set <int> tmpObjectListSector2 = g_ObjectListSector[trol][tcol].m_sectionlist;
			g_ObjectListSector[trol][tcol].m_lock.unlock();
			tmpObjectListSector.merge(tmpObjectListSector2);
		}

		trol = row + 1;
		tcol = col + 1;
		if ((players[p_id].m_x + 5) / (VIEW_RADIUS * 2) != row && (players[p_id].m_y + 5) / (VIEW_RADIUS * 2) != col
			&& (players[p_id].m_x + 5) < WORLD_WIDTH && (players[p_id].m_y + 5) < WORLD_HEIGHT)
		{
			g_ObjectListSector[trol][tcol].m_lock.lock();
			unordered_set <int> tmpObjectListSector2 = g_ObjectListSector[trol][tcol].m_sectionlist;
			g_ObjectListSector[trol][tcol].m_lock.unlock();
			tmpObjectListSector.merge(tmpObjectListSector2);
		}

		trol = row + 1;
		tcol = col - 1;
		if ((players[p_id].m_x + 5) / (VIEW_RADIUS * 2) != row && (players[p_id].m_y - 5) / (VIEW_RADIUS * 2) != col
			&& (players[p_id].m_x + 5) < WORLD_WIDTH && (players[p_id].m_y - 5) > 0)
		{
			g_ObjectListSector[trol][tcol].m_lock.lock();
			unordered_set <int> tmpObjectListSector2 = g_ObjectListSector[trol][tcol].m_sectionlist;
			g_ObjectListSector[trol][tcol].m_lock.unlock();
			tmpObjectListSector.merge(tmpObjectListSector2);
		}

		trol = row - 1;
		tcol = col + 1;
		if ((players[p_id].m_x - 5) / (VIEW_RADIUS * 2) != row && (players[p_id].m_y + 5) / (VIEW_RADIUS * 2) != col
			&& (players[p_id].m_x - 5) > 0 && (players[p_id].m_y + 5) < WORLD_HEIGHT)
		{
			g_ObjectListSector[trol][tcol].m_lock.lock();
			unordered_set <int> tmpObjectListSector2 = g_ObjectListSector[trol][tcol].m_sectionlist;
			g_ObjectListSector[trol][tcol].m_lock.unlock();
			tmpObjectListSector.merge(tmpObjectListSector2);
		}

		trol = row - 1;
		tcol = col - 1;
		if ((players[p_id].m_x - 5) / (VIEW_RADIUS * 2) != row && (players[p_id].m_y - 5) / (VIEW_RADIUS * 2) != col
			&& (players[p_id].m_x - 5) > 0 && (players[p_id].m_y - 5) > 0)
		{
			g_ObjectListSector[trol][tcol].m_lock.lock();
			unordered_set <int> tmpObjectListSector2 = g_ObjectListSector[trol][tcol].m_sectionlist;
			g_ObjectListSector[trol][tcol].m_lock.unlock();
			tmpObjectListSector.merge(tmpObjectListSector2);
		}



		for (auto& p : tmpObjectListSector) {
			if (p == p_id) continue;
			players[p].m_lock.lock();
			if (players[p].m_state != STATE_INGAME) {
				players[p].m_lock.unlock();
				continue;
			}
			if (can_see(p_id, p)) {
				players[p_id].m_vl.lock();
				players[p_id].m_viewlist.insert(p);
				players[p_id].m_vl.unlock();
				send_add_object(p_id, p);
				if (false == is_npc(p)) {
					players[p].m_vl.lock();
					players[p].m_viewlist.insert(p_id);
					players[p].m_vl.unlock();
					send_add_object(p, p_id);
				}
			}
			players[p].m_lock.unlock();
		}

		break;
	}
	case CS_MOVE: {
		cs_packet_move* move_packet = reinterpret_cast<cs_packet_move*>(packet);
		players[p_id].last_move_time = move_packet->move_time;
		player_move(p_id, move_packet->direction);
		break;
	}
	case CS_CHAT: {
		cs_packet_chat* chat_packet = reinterpret_cast<cs_packet_chat*>(packet);

		if (chat_packet->message[0] == '/') {
			string as(chat_packet->message);
			istringstream ss(as);
			string stringBuff;
			vector<string> x;
			while (getline(ss, stringBuff, ' '))
				x.push_back(stringBuff);

			if (x[0] == "/party") {
				if (x.size() == 2)
				{
					for (int i = 0; i < NPC_ID_START; ++i)
					{
						players[i].m_lock.lock();
						string s(players[i].m_name);
						players[i].m_lock.unlock();
						if (s == x[1])
						{
							send_party_invite(i, p_id);
							break;
						}
					}
					//string txt = "[파티]" + x[1] + "님은 없습니다.";
					//send_chat(p_id, p_id, txt.c_str());
				}else {
					string txt = "[명령어] /party [플레이어]";
					send_chat(p_id, p_id, txt.c_str());
				}
			}

			if (x[0] == "/pos") {
				if (x.size() == 3)
				{
					string txt = "(" + x[1] + ", " + x[2] + ") 좌표로 이동하였습니다!";
					send_chat(p_id, p_id, txt.c_str());
					player_teleport(p_id, stoi(x[1]), stoi(x[2]));
				}else {
					string txt = "[명령어] /pos [x좌표] [y좌표] ";
					send_chat(p_id, p_id, txt.c_str());
				}
			}
		}else {
			for (int i = 0; i < NPC_ID_START; ++i) {
				if (STATE_INGAME != players[i].m_state) {
					continue;
				}
				string txt = "[" + string(players[p_id].m_name) + "]" + ": " + chat_packet->message;
				cout << "[채팅]" << txt << endl;
				send_chat(players[i].m_id, p_id, txt.c_str());
			}
		}
		break;
	}
	case CS_ATTACK: {
		cs_packet_move* move_packet = reinterpret_cast<cs_packet_move*>(packet);
		//players[p_id].last_move_time = move_packet->move_time;
		player_attack(p_id);
		break;
	}
	case CS_PARTY_INVITE: {
		cs_packet_party_invite* my_packet = reinterpret_cast<cs_packet_party_invite*>(packet);
		send_party_invite(my_packet->id, p_id);
		break;
	}
	case CS_PARTY_ACCEPT: {
		cs_packet_party_accept* my_packet = reinterpret_cast<cs_packet_party_accept*>(packet);
		int invite = my_packet->id;
		int accept = p_id;
		players[accept].m_pl.lock();
		auto plist = players[accept].m_partylist;
		players[accept].m_pl.unlock();

		players[invite].m_pl.lock();
		auto plist2 = players[invite].m_partylist;
		players[invite].m_pl.unlock();

		for (const auto& p : plist2) {
			plist.insert(p);
			send_party_join(accept, p);
			send_add_object(accept, p);
			send_party_join(p, accept);
			send_add_object(p, accept);
			players[p].m_pl.lock();
			players[p].m_partylist.insert(accept);
			players[p].m_pl.unlock();
		}
		plist.insert(invite);
		send_party_join(accept, invite);
		send_add_object(accept, invite);

		players[accept].m_pl.lock();
		players[accept].m_partylist = plist;
		players[accept].m_pl.unlock();

		players[invite].m_pl.lock();
		players[invite].m_partylist.insert(accept);
		players[invite].m_pl.unlock();
		send_party_join(invite, accept);
		send_add_object(invite, accept);
		break;
	}
	case CS_PARTY_DENY: {
		cs_packet_party_deny* my_packet = reinterpret_cast<cs_packet_party_deny*>(packet);
		string txt = "[파티]" + string(players[my_packet->id].m_name) + "님이 파티 신청을 거절하였습니다.";
		send_chat(p_id, p_id, txt.c_str());
		break;
	}
	case CS_ITEM_USE: {
		cs_packet_item_use* my_packet = reinterpret_cast<cs_packet_item_use*>(packet);
		char item_id = my_packet->id;
		switch (item_id)
		{
			case 0: {
				players[p_id].m_il.lock();
				if (players[p_id].m_item.hp_postion > 0)
				{
					players[p_id].m_item.hp_postion -= 1;
					players[p_id].m_il.unlock();
					players[p_id].m_lock.lock();
					if(players[p_id].m_hp > 50)
						players[p_id].m_hp = 100;
					else
						players[p_id].m_hp += 50;
					players[p_id].m_lock.unlock();
					
					players[p_id].m_vl.lock();
					unordered_set <int> old_vl = players[p_id].m_viewlist;
					players[p_id].m_vl.unlock();

					for (auto& p : old_vl)
						send_stat_change(p, p_id);
					send_stat_change(p_id, p_id);

					players[p_id].m_pl.lock();
					unordered_set<int> pl = players[p_id].m_partylist;
					players[p_id].m_pl.unlock();

					player_give_party_stat_info(p_id, pl);

					string txt = "[회복] hp 포션을 사용하여 체력을 회복하였습니다.";
					send_chat(p_id, p_id, txt.c_str());
				}
				else {
					players[p_id].m_il.unlock();

					string txt = "[아이템] hp 포션 아이템이 없습니다.";
					send_chat(p_id, p_id, txt.c_str());
				}
				send_item_change(p_id, players[p_id].m_item);
				break;
			}
			case 1: {
				players[p_id].m_il.lock();
				if (players[p_id].m_item.mp_postion > 0)
				{
					players[p_id].m_item.mp_postion -= 1;
					players[p_id].m_il.unlock();
					players[p_id].m_lock.lock();
					if (players[p_id].m_mp > 50)
						players[p_id].m_mp = 100;
					else
						players[p_id].m_mp += 50;
					players[p_id].m_lock.unlock();

					players[p_id].m_vl.lock();
					unordered_set <int> old_vl = players[p_id].m_viewlist;
					players[p_id].m_vl.unlock();

					for (auto& p : old_vl)
						send_stat_change(p, p_id);
					send_stat_change(p_id, p_id);

					players[p_id].m_pl.lock();
					unordered_set<int> pl = players[p_id].m_partylist;
					players[p_id].m_pl.unlock();

					player_give_party_stat_info(p_id, pl);

					string txt = "[회복] mp 포션을 사용하여 마력을 회복하였습니다.";
					send_chat(p_id, p_id, txt.c_str());
				}
				else {
					players[p_id].m_il.unlock();

					string txt = "[아이템] mp 포션 아이템이 없습니다.";
					send_chat(p_id, p_id, txt.c_str());
				}
				send_item_change(p_id, players[p_id].m_item);
				break;
			}
			default: {
				break;
			}
		}
		break;
	}
	case CS_SKIL_USE: {
		players[p_id].m_lock.lock();
		if (players[p_id].m_mp - 30 <= 0)
		{
			players[p_id].m_lock.unlock();
			string txt = "[스킬] 마나가 부족합니다(마나 30필요)";
			send_chat(p_id, p_id, txt.c_str());
		}
		else {
			players[p_id].m_lock.unlock();

			players[p_id].m_skil_lock.lock();
			if (players[p_id].coolTime > 0) {
				players[p_id].m_skil_lock.unlock();
				string txt = "[스킬] 스킬 쿨타임" + to_string(players[p_id].coolTime) + "초 남았습니다.";
				send_chat(p_id, p_id, txt.c_str());
			}
			else
			{
				players[p_id].coolTime = 10;
				players[p_id].m_skil_lock.unlock();

				players[p_id].m_vl.lock();
				unordered_set<int> vl = players[p_id].m_viewlist;
				players[p_id].m_vl.unlock();

				players[p_id].m_lock.lock();
				if (players[p_id].m_hp > 30)
					players[p_id].m_hp = 100;
				else
					players[p_id].m_hp += 30;
				players[p_id].m_mp -= 30;
				players[p_id].m_lock.unlock();
				send_stat_change(p_id, p_id);
				for (const auto& p : vl)
				{
					if (is_npc(p)) continue;
					if (can_buf(p_id, p))
					{
						players[p].m_lock.lock();
						if (players[p].m_hp > 30)
							players[p].m_hp = 100;
						else
							players[p].m_hp += 30;
						players[p].m_lock.unlock();
						send_stat_change(p, p);
						send_stat_change(p_id, p);
						send_stat_change(p, p_id);
					}
				}

				players[p_id].m_pl.lock();
				unordered_set<int> pl = players[p_id].m_partylist;
				players[p_id].m_pl.unlock();
				player_give_party_stat_info(p_id, pl);
				string txt = "[스킬] 회복 버프 스킬을 사용하였습니다!";
				send_chat(p_id, p_id, txt.c_str());

				add_event(p_id, OP_PLAYER_SKIL_COOL, 1000);
			}
		}
		break;
	}
	default :
		cout << "Unknown Packet Type [" << p->type << "] Error\n";
		exit(-1);
		break;
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
	for (int i = 0; i < NPC_ID_START; ++i) {
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
	players[p_id].m_vl.lock();
	unordered_set <int> old_vl = players[p_id].m_viewlist;
	players[p_id].m_vl.unlock();

	players[p_id].m_lock.lock();
	save_player(p_id);
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
	party_leave(p_id);
	cout << "[퇴장]" << players[p_id].m_name << "님께서 퇴장하셨습니다." << endl;
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
			do_random_move_npc(key);
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
	if (players[id].moving)
		return;
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

	unordered_set <int> old_vl;
	unordered_set <int> tmpObjectListSector = get_sector(id, old_vl, x, y);

	unordered_set <int> new_vl;

	bool in_player = false;
	for (auto& p : tmpObjectListSector) {
		if (is_npc(p) == true) continue;
		if (STATE_INGAME != players[p].m_state) continue;
		if (true == can_see(id, p))
			new_vl.insert(p);
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
			in_player = true;
		}
	}
	for (auto pl : new_vl) {
		if (0 == old_vl.count(pl)) {
			players[pl].m_vl.lock();
			players[pl].m_viewlist.insert(id);
			players[pl].m_vl.unlock();
			send_add_object(pl, id);
			in_player = true;
		}
	}

	for (auto p : new_vl) {
		if (is_npc(p)) continue;
		EX_OVER* ex_over = new EX_OVER;
		ex_over->m_op = OP_PLAYER_MOVE;
		ex_over->m_traget_id = p;
		PostQueuedCompletionStatus(h_iocp, 1, id, &ex_over->m_over);
	}

	if (in_player && players[id].moving == false) {
		add_event(id, OP_RANDOM_MOVE, 1000);
		players[id].moving = true;
	}
}

void do_player_move_npc(int id)
{
	if (players[id].moving)
		return;
	int x = players[id].m_x;
	int y = players[id].m_y;
	switch (rand() % 4) {
	case 0: if (x > 0) x--; break;
	case 1: if (x < (WORLD_WIDTH - 1)) x++; break;
	case 2: if (y > 0) y--; break;
	case 3: if (y < (WORLD_HEIGHT - 1)) y++; break;
	}
	players[id].m_x = x;
	players[id].m_y = y;

	unordered_set <int> old_vl;
	unordered_set <int> tmpObjectListSector = get_sector(id, old_vl, x, y);

	unordered_set <int> new_vl;

	bool in_player = false;
	for (auto& p : tmpObjectListSector) {
		if (is_npc(p) == true) continue;
		if (STATE_INGAME != players[p].m_state) continue;
		if (true == can_see(id, p))
			new_vl.insert(p);
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
			in_player = true;
		}
	}
	for (auto pl : new_vl) {
		if (0 == old_vl.count(pl)) {
			players[pl].m_vl.lock();
			players[pl].m_viewlist.insert(id);
			players[pl].m_vl.unlock();
			send_add_object(pl, id);
			in_player = true;
		}
	}

	for (auto p : new_vl) {
		if (is_npc(p)) continue;
		EX_OVER* ex_over = new EX_OVER;
		ex_over->m_op = OP_PLAYER_MOVE;
		ex_over->m_traget_id = p;
		PostQueuedCompletionStatus(h_iocp, 1, id, &ex_over->m_over);
	}

	if (in_player && players[id].moving == false) {
		add_event(id, OP_RANDOM_MOVE, 1000);
		players[id].moving = true;
	}
}

void do_timer()
{
	using namespace chrono;

	for (;;) {
		timer_lock.lock();
		if (timer_queue.empty()) {
			timer_lock.unlock();
			continue;
		}
		if (timer_queue.top().exec_time > system_clock::now()) {
			timer_lock.unlock();
			this_thread::sleep_for(10ms);
		}
		else {
			auto ev = timer_queue.top();
			timer_queue.pop();
			timer_lock.unlock();
			switch (ev.event_type) {
				case OP_RANDOM_MOVE: {
					EX_OVER* ex_over = new EX_OVER;
					ex_over->m_op = OP_RANDOM_MOVE;
					PostQueuedCompletionStatus(h_iocp, 1, ev.object_id, &ex_over->m_over);
					players[ev.object_id].moving = false;
					//do_random_move_npc(ev.object_id);
					//add_event(ev.object_id, OP_RANDOM_MOVE, 1000);
					break;
				}
				case OP_NPC_RESPAWN: {
					players[ev.object_id].m_lock.lock();
					players[ev.object_id].m_state = STATE_INGAME;
					players[ev.object_id].m_hp = 100;
					players[ev.object_id].m_lock.unlock();

					players[ev.object_id].m_vl.lock();
					unordered_set <int> old_vl = players[ev.object_id].m_viewlist;
					players[ev.object_id].m_vl.unlock();
					for (auto& v : old_vl) {
						if (is_npc(v)) continue;
						send_add_object(v, ev.object_id);
					}
					break;
				}
				/*case OP_PLAYER_HEALING: {
					players[ev.object_id].m_lock.try_lock();
					if(players[ev.object_id].m_state != STATE_INGAME)
						players[ev.object_id].m_lock.unlock();
					else {
						if (players[ev.object_id].m_hp < 100) {
							if (int(players[ev.object_id].m_hp * 1 / 10) < 100)
								players[ev.object_id].m_hp += int(players[ev.object_id].m_hp * 1 / 10);
							else
								players[ev.object_id].m_hp = 100;
							players[ev.object_id].m_lock.unlock();

							players[ev.object_id].m_vl.lock();
							unordered_set <int> old_vl = players[ev.object_id].m_viewlist;
							players[ev.object_id].m_vl.unlock();

							for (auto& p : old_vl)
								send_stat_change(p, ev.object_id);
							send_stat_change(ev.object_id, ev.object_id);

							players[ev.object_id].m_pl.lock();
							unordered_set<int> pl = players[ev.object_id].m_partylist;
							players[ev.object_id].m_pl.unlock();

							player_give_party_stat_info(ev.object_id, pl);
						}
						add_event(ev.object_id, OP_PLAYER_HEALING, 5000);
					}
					break;
				}*/
				//case OP_PLAYER_SAVE: {
				//	players[ev.object_id].m_lock.try_lock();
				//	if (players[ev.object_id].m_state != STATE_INGAME)
				//		players[ev.object_id].m_lock.unlock();
				//	else {
				//		save_player(ev.object_id);
				//		add_event(ev.object_id, OP_PLAYER_SAVE, 60000);
				//	}
				//	break;
				//}
				case OP_PLAYER_SKIL_COOL: {
					players[ev.object_id].m_skil_lock.lock();

					if (players[ev.object_id].coolTime > 0)
					{
						players[ev.object_id].coolTime -= 1;
						players[ev.object_id].m_skil_lock.unlock();
						add_event(ev.object_id, OP_PLAYER_SKIL_COOL, 1000);
					}
					else {
						players[ev.object_id].m_skil_lock.unlock();
					}
				}
				default: {
					break;
				}
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

int API_damage_player(lua_State* L)
{
	int obj_id = lua_tonumber(L, -2);
	int p_id = lua_tonumber(L, -1);
	lua_pop(L, 3);
	players[p_id].m_lock.lock();
	players[p_id].m_hp -= 10;
	if (players[p_id].m_hp <= 0) {
		players[p_id].m_exp /= 2;
		players[p_id].m_hp = 100;
		players[p_id].m_lock.unlock();

		int x = rand() % 500;
		int y = rand() % 500;
		player_teleport(p_id, x, y);
		string txt = "[죽음] 죽었습니다.";
		send_chat(p_id, p_id, txt.c_str());
	} else 
		players[p_id].m_lock.unlock();

	players[p_id].m_vl.lock();
	unordered_set<int> vl = players[p_id].m_viewlist;
	players[p_id].m_vl.unlock();

	for (auto& p : vl) {
		if (is_npc(p)) continue;
		send_stat_change(p, p_id);
	}
	send_stat_change(p_id, p_id);

	players[p_id].m_pl.lock();
	unordered_set<int> pl = players[p_id].m_partylist;
	players[p_id].m_pl.unlock();

	player_give_party_stat_info(p_id, pl);

	string txt = "[전투]" + string(players[obj_id].m_name) + "에게 데미지 10을 받았다!";
	send_chat(p_id, p_id, txt.c_str());
	return 0;
}

int main()
{
	wcout.imbue(locale("korean"));
	setlocale(LC_ALL, "korean");

	for (int i = 0; i < MAX_USER;++i) {
		auto& pl = players[i];
		pl.m_id = i;
		pl.m_state = STATE_FREE;
		pl.last_move_time = 0;
		pl.m_obj_class = OB_PLAYER;
		// pl.m_viewlist.clear();
		
		if (true == is_npc(i)) {
			
			pl.m_x = rand() % WORLD_WIDTH;
			pl.m_y = rand() % WORLD_HEIGHT;
			if(i < MAX_VILIGER + NPC_ID_START)
				pl.m_obj_class = rand() % 2 + OB_VILIGER;
			else
				pl.m_obj_class = rand() % (OB_END - OB_MONSTER - 1) + OB_MONSTER + 1;

			switch (pl.m_obj_class)
			{
				case OB_VILIGER: {
					pl.m_hp = 100;
					pl.m_level = 1;
					pl.m_state = STATE_INGAME;
					string s = "시민";
					strcpy_s(pl.m_name, s.c_str());
					break;
				}
				case OB_GQUESTOR: {
					pl.m_hp = 100;
					pl.m_level = 1;
					pl.m_state = STATE_INGAME;
					string s = "마을촌장";
					strcpy_s(pl.m_name, s.c_str());
					break;
				}
				case OB_GOBBLINE: {
					pl.m_hp = 100;
					pl.m_level = 1;
					pl.m_state = STATE_INGAME;
					string s = "유령";
					strcpy_s(pl.m_name, s.c_str());
					pl.L = luaL_newstate();
					luaL_openlibs(pl.L);
					luaL_loadfile(pl.L, "monster_ai.lua");
					lua_pcall(pl.L, 0, 0, 0);

					lua_getglobal(pl.L, "set_o_id");
					lua_pushnumber(pl.L, i);
					lua_pcall(pl.L, 1, 0, 0);

					lua_register(pl.L, "API_send_message", API_send_message);
					lua_register(pl.L, "API_damage_player", API_damage_player);
					lua_register(pl.L, "API_get_x", API_get_x);
					lua_register(pl.L, "API_get_y", API_get_y);
					break;
				}
				case OB_ONI: {
					pl.m_hp = 100;
					pl.m_level = 2;
					pl.m_state = STATE_INGAME;
					string s = "사신";
					strcpy_s(pl.m_name, s.c_str());
					pl.L = luaL_newstate();
					luaL_openlibs(pl.L);
					luaL_loadfile(pl.L, "monster_ai.lua");
					lua_pcall(pl.L, 0, 0, 0);

					lua_getglobal(pl.L, "set_o_id");
					lua_pushnumber(pl.L, i);
					lua_pcall(pl.L, 1, 0, 0);

					lua_register(pl.L, "API_damage_player", API_damage_player);
					lua_register(pl.L, "API_get_x", API_get_x);
					lua_register(pl.L, "API_get_y", API_get_y);
					break;
				}
				case OB_GHOST: {
					pl.m_hp = 100;
					pl.m_level = 3;
					pl.m_state = STATE_INGAME;
					string s = "뱀파이어";
					strcpy_s(pl.m_name, s.c_str());
					pl.L = luaL_newstate();
					luaL_openlibs(pl.L);
					luaL_loadfile(pl.L, "monster_ai.lua");
					lua_pcall(pl.L, 0, 0, 0);

					lua_getglobal(pl.L, "set_o_id");
					lua_pushnumber(pl.L, i);
					lua_pcall(pl.L, 1, 0, 0);

					lua_register(pl.L, "API_damage_player", API_damage_player);
					lua_register(pl.L, "API_get_x", API_get_x);
					lua_register(pl.L, "API_get_y", API_get_y);
					break;
				}
				default:
					break;
			}

			// 섹터 영역 구하기
			int row = pl.m_x / (VIEW_RADIUS * 2);
			int col = pl.m_y / (VIEW_RADIUS * 2);

			g_ObjectListSector[row][col].m_lock.lock();
			g_ObjectListSector[row][col].m_sectionlist.insert(i);
			g_ObjectListSector[row][col].m_lock.unlock();
		}
		
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

	thread timer_thread{ do_timer };
	timer_thread.join();
	
	for (auto& th : worker_threads) th.join();
	closesocket(listenSocket);
	WSACleanup();
}


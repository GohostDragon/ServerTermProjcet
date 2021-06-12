myid = -1

function set_o_id(x )
	myid = x
end

function event_player_move(p_id)
	p_x = API_get_x(p_id)
	p_y = API_get_y(p_id)
	m_x = API_get_x(myid)
	m_y = API_get_y(myid)
	if (p_x == m_x) then
		if (p_y == m_y) then
			API_damage_player(myid, p_id);
		end
	end
end
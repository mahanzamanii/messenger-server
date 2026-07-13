const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mysql = require('mysql2/promise');

const app = express();
const server = http.createServer(app);
// ================= فیکس ارور CORS =================
const io = new Server(server, {
    cors: {
        origin: "*", // به همه اجازه میده، چون نشست ها تو api.php چک میشن نه تو socket
        methods: ["GET", "POST"]
    }
});

const db = mysql.createPool({ 
    host: process.env.DB_HOST || 'localhost', 
    user: process.env.DB_USER || 'root', 
    password: process.env.DB_PASS || '', 
    database: process.env.DB_NAME || 'messenger',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
    enableKeepAlive: true,
    keepAliveInitialDelay: 0
});
const onlineUsers = {};

io.on('connection', (socket) => {
    socket.on('user_connected', async (userId) => {
        onlineUsers[String(userId)] = socket.id;
        
        // آپدیت last_seen با مدیریت خطا
        db.execute('UPDATE users SET last_seen = NOW() WHERE id = ?', [userId]).catch(err => console.error("DB Update Error:", err.message));

        try {
            const [rows] = await db.execute('SELECT hide_online FROM users WHERE id = ?', [userId]);
            const isHidden = rows[0] && rows[0].hide_online == 1;
            if (!isHidden) io.emit('user_status', { user_id: userId, status: 'online' });
        } catch (err) { console.error("DB Select Error:", err.message); }

        try {
            const [chats] = await db.execute('SELECT chat_id FROM chat_members WHERE user_id = ?', [userId]);
            chats.forEach(c => socket.join(`chat_${c.chat_id}`));
        } catch (err) { console.error("DB Join Error:", err.message); }
    });
    
    socket.on('join_chat', (chatId) => socket.join(`chat_${chatId}`));

    socket.on('typing', (data) => socket.to(`chat_${data.chat_id}`).emit('status_update', { user_id: data.user_id, status: data.is_typing ? 'typing' : 'online' }));
    socket.on('recording', (data) => socket.to(`chat_${data.chat_id}`).emit('status_update', { user_id: data.user_id, status: data.is_recording ? 'recording' : 'online' }));

    socket.on('check_status', async (targetUserId) => {
        const isOnline = !!onlineUsers[targetUserId];
        if (isOnline) {
            try {
                const [rows] = await db.execute('SELECT hide_online FROM users WHERE id = ?', [targetUserId]);
                const isHidden = rows[0] && rows[0].hide_online == 1;
                if (isHidden) socket.emit('user_status', { user_id: targetUserId, status: 'offline', last_seen: new Date() });
                else socket.emit('user_status', { user_id: targetUserId, status: 'online' });
            } catch (err) { console.error(err); }
        } else {
            db.execute('SELECT last_seen FROM users WHERE id = ?', [targetUserId]).then(([rows]) => {
                if (rows.length > 0) socket.emit('user_status', { user_id: targetUserId, status: 'offline', last_seen: rows[0].last_seen });
            });
        }
    });

    socket.on('send_message', async (data) => {
        try {
            // ================= فیکس محدودیت ارسال پیام در گروه =================
            const [grpRows] = await db.execute('SELECT c.can_members_send, cm.role FROM chats c JOIN chat_members cm ON c.id = cm.chat_id WHERE c.id = ? AND cm.user_id = ?', [data.chat_id, data.sender_id]);
            if (grpRows.length > 0) {
                const canSend = grpRows[0].can_members_send == 1 || grpRows[0].canSend == true;
                const role = grpRows[0].role;
                // اگه گروه بسته باشه و کاربر ممبر عادی باشه، پیامش رو بلاک کن
                if (!canSend && role === 'member') {
                    return socket.emit('message_blocked', { chat_id: data.chat_id, temp_id: data.tempId });
                }
            }

            const [chatRows] = await db.execute('SELECT blocked_by FROM chats WHERE id = ?', [data.chat_id]);
            if (chatRows.length > 0 && chatRows[0].blocked_by !== null) {
                socket.emit('message_blocked', { chat_id: data.chat_id, temp_id: data.tempId });
                return;
            }

            const [result] = await db.execute(
                'INSERT INTO messages (chat_id, sender_id, text, file_url, file_type, file_name, reply_to) VALUES (?, ?, ?, ?, ?, ?, ?)',
                [data.chat_id, data.sender_id, data.text, data.file_url || null, data.file_type || null, data.file_name || null, data.reply_to || null]
            );
            data.id = result.insertId;
            data.is_deleted = false;
            data.reactions = null;
            await db.execute('UPDATE chat_members SET is_hidden = 0 WHERE chat_id = ?', [data.chat_id]);

            const [members] = await db.execute('SELECT user_id FROM chat_members WHERE chat_id = ?', [data.chat_id]);
            members.forEach(member => {
                const memberId = String(member.user_id);
                const targetSocketId = onlineUsers[memberId];
                if (targetSocketId) {
                    io.sockets.sockets.get(targetSocketId)?.join(`chat_${data.chat_id}`);
                }
            });

            io.to(`chat_${data.chat_id}`).emit('new_message', data);
            socket.to(`chat_${data.chat_id}`).emit('refresh_sidebar');

        } catch (err) { console.error("Send Error:", err); }
    });

    // ================= سیستم ریکشن Real-time =================
    socket.on('react_message', async (data) => {
        try {
            const [rows] = await db.execute('SELECT reactions FROM messages WHERE id = ?', [data.message_id]);
            if (rows.length === 0) return;

            let reactions = rows[0].reactions ? rows[0].reactions.split(',') : [];
            reactions = reactions.filter(r => r); // پاک کردن خالی‌ها

            const userReactionIndex = reactions.findIndex(r => r.startsWith(`${data.user_id}:`));

            if (userReactionIndex !== -1) {
                // اگه قبلا لایک کرده بود و دوباره لایک کرد، حذف شه
                if (reactions[userReactionIndex] === `${data.user_id}:${data.emoji}`) {
                    reactions.splice(userReactionIndex, 1);
                } else {
                    // اگه ریکشن دیگه‌ای داده بود، عوض شه
                    reactions[userReactionIndex] = `${data.user_id}:${data.emoji}`;
                }
            } else {
                // اگه هیچ ریکشنی نداشت، اضافه شه
                reactions.push(`${data.user_id}:${data.emoji}`);
            }

            const reactionsStr = reactions.join(',');
            await db.execute('UPDATE messages SET reactions = ? WHERE id = ?', [reactionsStr, data.message_id]);

            io.to(`chat_${data.chat_id}`).emit('message_reacted', { message_id: data.message_id, chat_id: data.chat_id, reactions: reactionsStr });
        } catch (err) { console.error(err); }
    });

    socket.on('messages_read', async (data) => {
        try {
            // ۱. آپدیت کردن دیتابیس برای طرفی که پیام رو دیده
            await db.execute('UPDATE chat_members SET last_read_message_id = ? WHERE chat_id = ? AND user_id = ?', [data.last_read_id, data.chat_id, data.reader_id]);
            // ۲. به طرف مقابل بگو تیک‌ها رو آبی کن
            socket.to(`chat_${data.chat_id}`).emit('chat_read_by_user', data);
            // ۳. به خودمون بگو سایدبار رو آپدیت کن (تا کپسول تعداد پیام خوانده نشده پاک شه)
            socket.emit('update_sidebar');
        } catch (err) { console.error(err); }
    });

    socket.on('delete_message', async (data) => {
        try {
            // حذف کامل و واقعی پیام از دیتابیس
            await db.execute('DELETE FROM messages WHERE id = ?', [data.message_id]);
            // اطلاع دادن به هر دو طرف برای پاک کردن پیام از صفحه
            io.to(`chat_${data.chat_id}`).emit('message_deleted', { id: data.message_id, chat_id: data.chat_id });
        } catch (err) { console.error(err); }
    });

    // ================= آپدیت Real-time پروفایل =================
    socket.on('profile_updated', (data) => {
        // ارسال اطلاعات جدید پروفایل به تمام کاربران آنلاین
        socket.broadcast.emit('user_profile_updated', data);
    });

    // ================= سیستم بلاک کردن Real-time =================
    socket.on('toggle_block', (data) => {
        io.to(`chat_${data.chat_id}`).emit('chat_block_status', { chat_id: data.chat_id, is_blocked: data.is_blocked });
    });

    // پین کردن پیام (تغییر وضعیت)
    socket.on('pin_message', async (data) => {
        try {
            await db.execute('UPDATE messages SET is_pinned = NOT is_pinned WHERE id = ?', [data.message_id]);
            io.to(`chat_${data.chat_id}`).emit('message_pinned', { chat_id: data.chat_id, message_id: data.message_id });
        } catch (err) { console.error(err); }
    });

    // ================= پین کردن چندگانه (فیکس باگ منو سلکت) =================
    socket.on('bulk_pin_messages', async (data) => {
        try {
            // data.message_ids یک آرایه از آیدی پیام‌هاست
            for (let id of data.message_ids) {
                await db.execute('UPDATE messages SET is_pinned = NOT is_pinned WHERE id = ?', [id]);
            }
            // ارسال لیست آیدی‌ها به کلاینت‌ها برای آپدیت همزمان
            io.to(`chat_${data.chat_id}`).emit('messages_bulk_pinned', { chat_id: data.chat_id, message_ids: data.message_ids });
        } catch (err) { console.error(err); }
    });

    // ================= ادیت کردن پیام (با کنترل دسترسی گروه) =================
    socket.on('edit_message', async (data) => {
        try {
            const [msgRows] = await db.execute('SELECT sender_id FROM messages WHERE id = ?', [data.message_id]);
            if (msgRows.length === 0) return;

            const senderId = msgRows[0].sender_id;
            // اگه خودش نبود، چک کن آیا تو گروه ادمینه یا نه
            if (String(senderId) !== String(data.sender_id)) {
                const [memRows] = await db.execute("SELECT role FROM chat_members WHERE chat_id = ? AND user_id = ?", [data.chat_id, data.sender_id]);
                if (memRows.length === 0 || (memRows[0].role !== 'owner' && memRows[0].role !== 'admin')) return;
            }

            await db.execute('UPDATE messages SET text = ?, is_edited = TRUE WHERE id = ?', [data.newText, data.message_id]);
            io.to(`chat_${data.chat_id}`).emit('message_edited', { message_id: data.message_id, chat_id: data.chat_id, newText: data.newText });
        } catch (err) { console.error(err); }
    });

    // ================= حذف پیام (با کنترل دسترسی گروه) =================
    socket.on('delete_messages', async (data) => {
        try {
            for (let id of data.message_ids) {
                const [msgRows] = await db.execute('SELECT sender_id FROM messages WHERE id = ?', [id]);
                if (msgRows.length === 0) continue;

                const senderId = msgRows[0].sender_id;
                // اگه خودش نبود، چک کن آیا ادمین/اونر هست
                if (String(senderId) !== String(data.sender_id)) {
                    const [memRows] = await db.execute("SELECT role FROM chat_members WHERE chat_id = ? AND user_id = ?", [data.chat_id, data.sender_id]);
                    if (memRows.length === 0 || (memRows[0].role !== 'owner' && memRows[0].role !== 'admin')) continue;
                }

                await db.execute('DELETE FROM messages WHERE id = ?', [id]);
            }
            io.to(`chat_${data.chat_id}`).emit('messages_deleted', { ids: data.message_ids, chat_id: data.chat_id });
        } catch (err) { console.error(err); }
    });

    // فوروارد کردن پیام‌ها
    socket.on('forward_messages', async (data) => {
        try {
            for (let msg of data.messages) {
                const [result] = await db.execute(
                    'INSERT INTO messages (chat_id, sender_id, text, file_url, file_type, file_name, forwarded_from) VALUES (?, ?, ?, ?, ?, ?, ?)',
                    [
                        data.target_chat_id, data.sender_id, msg.text,
                        msg.file_url || null, msg.file_type || null, msg.file_name || null,
                        msg.forwarded_from || null
                    ]
                );

                io.to(`chat_${data.target_chat_id}`).emit('new_message', {
                    id: result.insertId, chat_id: data.target_chat_id, sender_id: data.sender_id,
                    sender_name: data.sender_name, sender_color: data.sender_color,
                    text: msg.text, forwarded_from: msg.forwarded_from,
                    file_url: msg.file_url, file_type: msg.file_type, file_name: msg.file_name,
                    created_at: new Date().toISOString()
                });
            }
        } catch (err) { console.error("Forward Error:", err); }
    });

    socket.on('disconnect', () => {
        for (const id in onlineUsers) {
            if (onlineUsers[id] === socket.id) {
                delete onlineUsers[id];
                db.execute('UPDATE users SET last_seen = NOW() WHERE id = ?', [id]);
                io.emit('user_status', { user_id: id, status: 'offline', last_seen: new Date() });
                break;
            }
        }
    });

    // ================= WebRTC Signaling =================
    socket.on('call_user', (data) => {
        const targetSocket = onlineUsers[String(data.target_user_id)];
        if (targetSocket) {
            io.to(targetSocket).emit('incoming_call', {
                from: data.from,
                from_name: data.from_name,
                call_type: data.call_type,
                signal: data.signal
            });
        }
    });

    socket.on('call_answer', (data) => {
        const targetSocket = onlineUsers[String(data.target_user_id)];
        if (targetSocket) io.to(targetSocket).emit('call_accepted', { signal: data.signal });
    });

    socket.on('call_reject', (data) => {
        const targetSocket = onlineUsers[String(data.target_user_id)];
        if (targetSocket) io.to(targetSocket).emit('call_rejected');
    });

    socket.on('call_ice', (data) => {
        const targetSocket = onlineUsers[String(data.target_user_id)];
        if (targetSocket) io.to(targetSocket).emit('call_ice', { candidate: data.candidate });
    });

    socket.on('call_end', (data) => {
        const targetSocket = onlineUsers[String(data.target_user_id)];
        if (targetSocket) io.to(targetSocket).emit('call_ended');
    });

    // ================= لاگ کردن تماس در دیتابیس =================
    socket.on('log_call', async (data) => {
        try {
            const [result] = await db.execute(
                'INSERT INTO messages (chat_id, sender_id, is_call_log, call_duration, call_type) VALUES (?, ?, 1, ?, ?)',
                [data.chat_id, data.sender_id, data.duration, data.call_type]
            );
            // ارسال پیام لاگ تماس به هر دو طرف
            io.to(`chat_${data.chat_id}`).emit('new_message', {
                id: result.insertId,
                chat_id: data.chat_id,
                sender_id: data.sender_id,
                sender_name: data.sender_name,
                sender_color: data.sender_color,
                is_call_log: true,
                call_duration: data.duration,
                call_type: data.call_type,
                created_at: new Date().toISOString()
            });
        } catch (err) { console.error(err); }
    });

});


server.listen(3000, () => console.log('🚀 Server running on 3000'));

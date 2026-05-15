import 'dart:convert';
import 'dart:io';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart' show rootBundle;
import 'package:path_provider/path_provider.dart';
import 'package:http/http.dart' as http;
import 'package:web_socket_channel/web_socket_channel.dart';

void main() {
  runApp(const MessengerApp());
}

class MessengerApp extends StatelessWidget {
  const MessengerApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Mesh-CU',
      theme: ThemeData(
        brightness: Brightness.dark,
        primarySwatch: Colors.blue,
        scaffoldBackgroundColor: const Color(0xFF1E1E2E),
        appBarTheme: const AppBarTheme(backgroundColor: Color(0xFF181825)),
      ),
      home: const ChatListScreen(),
    );
  }
}

class BackendService {
  static final BackendService _instance = BackendService._internal();
  factory BackendService() => _instance;
  BackendService._internal();

  Process? _process;
  bool _isReady = false;
  WebSocketChannel? _channel;
  
  final String apiUrl = 'http://127.0.0.1:8081/api';
  final String wsUrl = 'ws://127.0.0.1:8081/ws';

  Function(dynamic)? onMessage;
  Function(dynamic)? onPeerEvent;

  Future<void> start() async {
    if (_isReady) return;

    if (Platform.isLinux || Platform.isWindows || Platform.isMacOS) {
      try {
        final dir = await getApplicationSupportDirectory();
        final execName = Platform.isWindows ? 'server_bin.exe' : 'server_bin';
        final execPath = '${dir.path}/$execName';
        
        final execFile = File(execPath);
        if (!await execFile.exists()) {
          final data = await rootBundle.load('assets/server_bin');
          final bytes = data.buffer.asUint8List(data.offsetInBytes, data.lengthInBytes);
          await execFile.writeAsBytes(bytes);
          if (!Platform.isWindows) {
            await Process.run('chmod', ['+x', execPath]);
          }
        }

        _process = await Process.start(execPath, []);
        _process?.stdout.transform(utf8.decoder).listen((data) => print('[Go] $data'));
        _process?.stderr.transform(utf8.decoder).listen((data) => print('[Go Error] $data'));
        
        await Future.delayed(const Duration(seconds: 2));
        _connectWs();
        _isReady = true;
      } catch (e) {
        print('Error starting backend: $e');
      }
    } else {
      print('Android execution requires Gomobile FFI (not included in this demo). Connecting to existing localhost server if any.');
      _connectWs();
      _isReady = true;
    }
  }

  void _connectWs() {
    try {
      _channel = WebSocketChannel.connect(Uri.parse(wsUrl));
      _channel!.stream.listen((message) {
        final event = jsonDecode(message);
        if (event['type'] == 'new_message') {
          if (onMessage != null) onMessage!(event['payload']);
        } else if (event['type'] == 'peer_joined' || event['type'] == 'peer_left') {
          if (onPeerEvent != null) onPeerEvent!(event['payload']);
        }
      }, onError: (e) {
        print('WS Error: $e');
        Future.delayed(const Duration(seconds: 2), _connectWs);
      });
    } catch (e) {
      print('WS Connect Error: $e');
    }
  }

  Future<List<dynamic>> getChats() async {
    final res = await http.get(Uri.parse('$apiUrl/chats'));
    if (res.statusCode == 200) {
      return jsonDecode(res.body) ?? [];
    }
    return [];
  }

  Future<List<dynamic>> getMessages(String chatId) async {
    final res = await http.get(Uri.parse('$apiUrl/messages?chat_id=$chatId'));
    if (res.statusCode == 200) {
      return jsonDecode(res.body) ?? [];
    }
    return [];
  }

  Future<void> sendMessage(String chatId, String chatType, String body) async {
    await http.post(
      Uri.parse('$apiUrl/send'),
      headers: {'Content-Type': 'application/json'},
      body: jsonEncode({
        'chat_id': chatId,
        'chat_type': chatType,
        'body': body,
      }),
    );
  }

  void dispose() {
    _channel?.sink.close();
    _process?.kill();
  }
}

class ChatListScreen extends StatefulWidget {
  const ChatListScreen({super.key});

  @override
  State<ChatListScreen> createState() => _ChatListScreenState();
}

class _ChatListScreenState extends State<ChatListScreen> {
  final backend = BackendService();
  List<dynamic> chats = [];
  bool isLoading = true;

  @override
  void initState() {
    super.initState();
    _init();
  }

  Future<void> _init() async {
    await backend.start();
    backend.onMessage = (payload) {
      _loadChats();
    };
    _loadChats();
  }

  Future<void> _loadChats() async {
    final res = await backend.getChats();
    setState(() {
      chats = res;
      isLoading = false;
    });
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Chats'),
        actions: [
          IconButton(
            icon: const Icon(Icons.refresh),
            onPressed: _loadChats,
          )
        ],
      ),
      body: isLoading
          ? const Center(child: CircularProgressIndicator())
          : ListView.builder(
              itemCount: chats.length,
              itemBuilder: (context, index) {
                final chat = chats[index];
                return ListTile(
                  leading: CircleAvatar(
                    backgroundColor: Colors.blueAccent,
                    child: Text(chat['name'][0].toUpperCase()),
                  ),
                  title: Text(chat['name']),
                  subtitle: Text(chat['chat_id']),
                  trailing: chat['unread'] > 0
                      ? CircleAvatar(
                          radius: 12,
                          backgroundColor: Colors.redAccent,
                          child: Text('${chat['unread']}', style: const TextStyle(fontSize: 12)),
                        )
                      : null,
                  onTap: () {
                    Navigator.push(
                      context,
                      MaterialPageRoute(
                        builder: (_) => ChatScreen(
                          chatId: chat['chat_id'],
                          chatName: chat['name'],
                          chatType: chat['chat_id'].startsWith('group:') ? 'group' : 'private',
                        ),
                      ),
                    ).then((_) => _loadChats());
                  },
                );
              },
            ),
      floatingActionButton: FloatingActionButton(
        onPressed: () {
          // Implement new chat / peers dialog here
        },
        child: const Icon(Icons.message),
      ),
    );
  }
}

class ChatScreen extends StatefulWidget {
  final String chatId;
  final String chatName;
  final String chatType;

  const ChatScreen({
    super.key,
    required this.chatId,
    required this.chatName,
    required this.chatType,
  });

  @override
  State<ChatScreen> createState() => _ChatScreenState();
}

class _ChatScreenState extends State<ChatScreen> {
  final backend = BackendService();
  final TextEditingController _controller = TextEditingController();
  List<dynamic> messages = [];

  @override
  void initState() {
    super.initState();
    _loadMessages();
    backend.onMessage = (payload) {
      if (payload['chat_id'] == widget.chatId) {
        setState(() {
          messages.add(payload);
        });
      }
    };
  }

  Future<void> _loadMessages() async {
    final res = await backend.getMessages(widget.chatId);
    setState(() {
      messages = res;
    });
  }

  void _send() {
    if (_controller.text.trim().isEmpty) return;
    backend.sendMessage(widget.chatId, widget.chatType, _controller.text);
    _controller.clear();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: Text(widget.chatName)),
      body: Column(
        children: [
          Expanded(
            child: ListView.builder(
              itemCount: messages.length,
              itemBuilder: (context, index) {
                final msg = messages[index];
                return ListTile(
                  title: Text(msg['sender_nick'] ?? msg['sender'] ?? 'Unknown', style: const TextStyle(color: Colors.blueAccent)),
                  subtitle: Text(msg['body']),
                );
              },
            ),
          ),
          Padding(
            padding: const EdgeInsets.all(8.0),
            child: Row(
              children: [
                Expanded(
                  child: TextField(
                    controller: _controller,
                    decoration: const InputDecoration(
                      hintText: 'Type a message...',
                      border: OutlineInputBorder(),
                    ),
                    onSubmitted: (_) => _send(),
                  ),
                ),
                IconButton(
                  icon: const Icon(Icons.send, color: Colors.blueAccent),
                  onPressed: _send,
                ),
              ],
            ),
          )
        ],
      ),
    );
  }
}

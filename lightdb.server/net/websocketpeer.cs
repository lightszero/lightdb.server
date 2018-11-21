using lightdb.sdk;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using LightDB;
namespace lightdb.server
{

    public class websockerPeer : lightchain.httpserver.httpserver.IWebSocketPeer
    {
        System.Net.WebSockets.WebSocket websocket;

        public websockerPeer(System.Net.WebSockets.WebSocket websocket)
        {
            this.websocket = websocket;
        }
        public async Task OnConnect()
        {
            Console.CursorLeft = 0;

            Console.WriteLine("websocket in:" + websocket.SubProtocol);
        }

        public async Task OnDisConnect()
        {
            Console.CursorLeft = 0;

            Console.WriteLine("websocket gone:" + websocket.CloseStatus + "." + websocket.CloseStatusDescription);

        }
        private async Task SendToClient(NetMessage msg)
        {
            await this.websocket.SendAsync(msg.ToBytes(), System.Net.WebSockets.WebSocketMessageType.Binary, true, System.Threading.CancellationToken.None);
        }
        public async Task OnRecv(System.IO.MemoryStream stream, int recvcount)
        {
            var p = stream.Position;
            var msg = NetMessage.Unpack(stream);
            var pend = stream.Position;
            if (pend - p > recvcount)
                throw new Exception("error net message.");

            var iddata = msg.Params["_id"];
            if (iddata.Length != 8)
                throw new Exception("error net message _id");

            switch (msg.Cmd)
            {
                case "_ping":
                    await OnPing(msg, iddata);
                    break;
                case "_db.state":
                    await OnDB_State(msg, iddata);
                    break;

                ///snapshot interface
                case "_db.usesnapshot":
                    await OnDB_UseSnapShot(msg, iddata);
                    break;
                case "_db.unusesnapshot":
                    await OnDB_UnuseSnapShot(msg, iddata);
                    break;
                case "_db.snapshot.dataheight":
                    await OnSnapshotDataHeight(msg, iddata);//ulong DataHeight { get; }
                    break;
                case "_db.snapshot.getvalue":
                    await OnSnapshot_GetValue(msg, iddata);//DBValue GetValue(byte[] tableid, byte[] key);
                    break;
                case "_db.snapshot.gettableinfo":
                    await OnSnapshot_GetTableInfo(msg, iddata);//TableInfo GetTableInfo(byte[] tableid);
                    break;
                case "_db.snapshot.gettablecount":
                    await OnSnapshot_GetTableCount(msg, iddata); //uint GetTableCount(byte[] tableid);
                    break;
                case "_db.snapshot.newiterator":
                    await OnSnapshot_CreateKeyIterator(msg, iddata); //CreateKeyIterator(byte[] tableid, byte[] _beginkey = null, byte[] _endkey = null);
                    break;
                case "_db.iterator.current":
                    await OnSnapshot_IteratorCurrent(msg, iddata); //CreateKeyIterator(byte[] tableid, byte[] _beginkey = null, byte[] _endkey = null);
                    break;
                case "_db.iterator.next":
                    await OnSnapshot_IteratorNext(msg, iddata); //CreateKeyIterator(byte[] tableid, byte[] _beginkey = null, byte[] _endkey = null);
                    break;
                case "_db.iterator.reset":
                    await OnSnapshot_IteratorReset(msg, iddata); //CreateKeyIterator(byte[] tableid, byte[] _beginkey = null, byte[] _endkey = null);
                    break;

                ///write method
                case "_db.wrtie":
                    await OnDB_Write(msg, iddata);
                    break;

                default:
                    throw new Exception("unknown msg cmd:" + msg.Cmd);
            }
        }
        public async Task OnPing(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_ping.back");
            msg.Params["_id"] = id;

            await SendToClient(msg);

        }
        public async Task OnDB_State(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.state.back");
            msg.Params["_id"] = id;
            msg.Params["dbopen"] = new byte[] { (byte)(Program.storage.state_DBOpen ? 1 : 0) };
            if (Program.storage.state_DBOpen)
            {
                using (var snap = Program.storage.maindb.UseSnapShot())
                {
                    msg.Params["height"] = BitConverter.GetBytes(snap.DataHeight);
                    //Console.WriteLine("dataheight=" + snap.DataHeight);
                    var value = snap.GetValue(LightDB.LightDB.systemtable_info, "_magic_".ToBytes_UTF8Encode());
                    if (value != null)
                        msg.Params["magic"] = value.value;

                    var keys = snap.CreateKeyFinder(StorageService.tableID_Writer);
                    var i = 0;
                    foreach (byte[] keybin in keys)
                    {
                        //var key = keybin.ToString_UTF8Decode();
                        var iswriter = snap.GetValue(StorageService.tableID_Writer, keybin).AsBool();
                        if (iswriter)
                        {
                            i++;
                            msg.Params["writer" + i] = keybin;
                            //Console.WriteLine("writer:" + key);
                        }
                    }
                }
            }
            await SendToClient(msg);

        }
        public async Task OnDB_UseSnapShot(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.usesnapshot.back");
            msg.Params["_id"] = id;
            msg.Params["_error"] = "not implement yet".ToBytes_UTF8Encode();
            await SendToClient(msg);
        }
        public async Task OnDB_UnuseSnapShot(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.unusesnapshot.back");
            msg.Params["_id"] = id;
            msg.Params["_error"] = "not implement yet".ToBytes_UTF8Encode();
            await SendToClient(msg);
        }
        public async Task OnSnapshotDataHeight(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.snapshot.dataheight.back");
            msg.Params["_id"] = id;
            msg.Params["_error"] = "not implement yet".ToBytes_UTF8Encode();
            await SendToClient(msg);
        }
        public async Task OnSnapshot_GetValue(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.snapshot.getvalue.back");
            msg.Params["_id"] = id;
            msg.Params["_error"] = "not implement yet".ToBytes_UTF8Encode();
            await SendToClient(msg);
        }
        public async Task OnSnapshot_GetTableCount(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.snapshot.gettablecount.back");
            msg.Params["_id"] = id;
            msg.Params["_error"] = "not implement yet".ToBytes_UTF8Encode();
            await SendToClient(msg);
        }
        public async Task OnSnapshot_GetTableInfo(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.snapshot.gettableinfo.back");
            msg.Params["_id"] = id;
            msg.Params["_error"] = "not implement yet".ToBytes_UTF8Encode();
            await SendToClient(msg);
        }
        public async Task OnSnapshot_CreateKeyIterator(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("__db.snapshot.newiterator.back");
            msg.Params["_id"] = id;
            msg.Params["_error"] = "not implement yet".ToBytes_UTF8Encode();
            if (Program.storage.state_DBOpen)
            {
                var snapshot = Program.storage.maindb.UseSnapShot();
                snapshot.CreateKeyIterator(msg.Params["tableid"], null, null);
            }
            await SendToClient(msg);
        }
        public async Task OnSnapshot_IteratorCurrent(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.iterator.current.back");
            msg.Params["_id"] = id;
            msg.Params["_error"] = "not implement yet".ToBytes_UTF8Encode();
            await SendToClient(msg);
        }
        public async Task OnSnapshot_IteratorNext(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.iterator.next.back");
            msg.Params["_id"] = id;
            msg.Params["_error"] = "not implement yet".ToBytes_UTF8Encode();
            await SendToClient(msg);
        }
        public async Task OnSnapshot_IteratorReset(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.iterator.reset.back");
            msg.Params["_id"] = id;
            msg.Params["_error"] = "not implement yet".ToBytes_UTF8Encode();
            await SendToClient(msg);
        }
        public async Task OnDB_Write(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.write.back");
            msg.Params["_id"] = id;
            msg.Params["_error"] = "not implement yet".ToBytes_UTF8Encode();
            await SendToClient(msg);
        }
    }
}

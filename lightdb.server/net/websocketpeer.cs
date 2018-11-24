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
            this.peerSnapshots = new System.Collections.Concurrent.ConcurrentDictionary<ulong, ISnapShot>();
            this.peerItertors = new System.Collections.Concurrent.ConcurrentDictionary<ulong, IEnumerator<byte[]>>();
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
            DisposeSnapShots();
        }
        public void DisposeSnapShots()
        {
            try
            {
                foreach (var psnap in peerSnapshots.Values)
                {
                    if (psnap != null)
                    {
                        try
                        {
                            psnap.Dispose();
                        }
                        catch
                        {

                        }
                    }
                }

                peerSnapshots.Values.Clear();
            }
            catch
            {

            }
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
            SendToClient(msg);

        }
        public async Task OnDB_State(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.state.back");
            msg.Params["_id"] = id;
            msg.Params["dbopen"] = new byte[] { (byte)(Program.storage.state_DBOpen ? 1 : 0) };
            if (Program.storage.state_DBOpen)
            {
                try
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
                catch (Exception err)
                {
                    msg.Params["_error"] = err.Message.ToBytes_UTF8Encode();
                }
            }
            else
            {
                msg.Params["_error"] = "db not open.".ToBytes_UTF8Encode();
            }
            SendToClient(msg);

        }

        //收集所有snapshot
        public System.Collections.Concurrent.ConcurrentDictionary<UInt64, ISnapShot> peerSnapshots;
        public System.Collections.Concurrent.ConcurrentDictionary<UInt64, IEnumerator<byte[]>> peerItertors;
        public async Task OnDB_UseSnapShot(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.usesnapshot.back");
            msg.Params["_id"] = id;
            try
            {
                UInt64? wantheight = msg.Params.ContainsKey("snapheight") ? (UInt64?)BitConverter.ToUInt64(msg.Params["snapheight"], 0) : null;
                if (wantheight == null)
                {
                    var snapshot = Program.storage.maindb.UseSnapShot();
                    wantheight = snapshot.DataHeight;
                    peerSnapshots[snapshot.DataHeight] = snapshot;
                }
                else
                {
                    if (peerSnapshots.ContainsKey(wantheight.Value) == false)
                    {
                        msg.Params["_error"] = "do not have that snapheight".ToBytes_UTF8Encode();
                    }
                }
                if (msg.Params.ContainsKey("_error") == false)
                {
                    msg.Params["snapheight"] = BitConverter.GetBytes(wantheight.Value);
                }
            }
            catch (Exception err)
            {
                msg.Params["_error"] = err.Message.ToBytes_UTF8Encode();
            }
            SendToClient(msg);
        }
        public async Task OnDB_UnuseSnapShot(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.unusesnapshot.back");
            msg.Params["_id"] = id;
            try
            {
                UInt64 snapheight = BitConverter.ToUInt64(msg.Params["snapheight"]);
                bool b = peerSnapshots.TryRemove(snapheight, out ISnapShot value);
                if (b)
                {
                    value.Dispose();
                }
                msg.Params["remove"] = new byte[] { (byte)(b ? 1 : 0) };
            }
            catch (Exception err)
            {
                msg.Params["_error"] = err.Message.ToBytes_UTF8Encode();
            }
            SendToClient(msg);
        }
        public async Task OnSnapshotDataHeight(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.snapshot.dataheight.back");
            msg.Params["_id"] = id;
            try
            {
                UInt64 snapheight = BitConverter.ToUInt64(msg.Params["snapheight"]);
                var snap = peerSnapshots[snapheight];
                msg.Params["dataheight"] = BitConverter.GetBytes(snap.DataHeight);

            }
            catch (Exception err)
            {
                msg.Params["_error"] = err.Message.ToBytes_UTF8Encode();
            }
            SendToClient(msg);
        }
        public async Task OnSnapshot_GetValue(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.snapshot.getvalue.back");
            msg.Params["_id"] = id;
            try
            {
                UInt64 snapheight = BitConverter.ToUInt64(msg.Params["snapheight"]);
                var snap = peerSnapshots[snapheight];
                byte[] data = snap.GetValueData(msg.Params["tableid"], msg.Params["key"]);
                msg.Params["data"] = data;
            }
            catch (Exception err)
            {
                msg.Params["_error"] = err.Message.ToBytes_UTF8Encode();
            }
            //这个完全可以不要等待呀
            SendToClient(msg);
        }
        public async Task OnSnapshot_GetTableCount(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.snapshot.gettablecount.back");
            msg.Params["_id"] = id;
            try
            {
                UInt64 snapheight = BitConverter.ToUInt64(msg.Params["snapheight"]);
                var snap = peerSnapshots[snapheight];
                byte[] data = BitConverter.GetBytes(snap.GetTableCount(msg.Params["tableid"]));
                msg.Params["count"] = data;
            }
            catch (Exception err)
            {
                msg.Params["_error"] = err.Message.ToBytes_UTF8Encode();
            }
            //这个完全可以不要等待呀
            SendToClient(msg);
        }
        public async Task OnSnapshot_GetTableInfo(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.snapshot.gettableinfo.back");
            msg.Params["_id"] = id;
            try
            {
                UInt64 snapheight = BitConverter.ToUInt64(msg.Params["snapheight"]);
                var snap = peerSnapshots[snapheight];

                //此处可以优化，增加一个GetTableInfoData,不要转一次
                byte[] data = snap.GetTableInfo(msg.Params["tableid"]).ToBytes();
                msg.Params["info"] = data;
            }
            catch (Exception err)
            {
                msg.Params["_error"] = err.Message.ToBytes_UTF8Encode();
            }
            //这个完全可以不要等待呀
            SendToClient(msg);
        }
        public async Task OnSnapshot_CreateKeyIterator(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("__db.snapshot.newiterator.back");
            msg.Params["_id"] = id;
            try
            {
                UInt64 snapheight = BitConverter.ToUInt64(msg.Params["snapheight"]);
                var snap = peerSnapshots[snapheight];

                //这里缺一个给唯一ID的方式,采用顺序编号是个临时方法
                var beginkey = msg.Params?["beginkey"];
                var endkey = msg.Params?["endkey"];
                var iter = snap.CreateKeyIterator(msg.Params["tableid"], beginkey, endkey);
                ulong index = (UInt64)this.peerItertors.Count;
                this.peerItertors[index] = iter;

                msg.Params["iteratorid"] = BitConverter.GetBytes(index);
            }
            catch (Exception err)
            {
                msg.Params["_error"] = err.Message.ToBytes_UTF8Encode();
            }
            //这个完全可以不要等待呀
            SendToClient(msg);

        }
        public async Task OnSnapshot_IteratorCurrent(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.iterator.current.back");
            msg.Params["_id"] = id;
            try
            {
                UInt64 iteratorid = BitConverter.ToUInt64(msg.Params["iteratorid"]);
                var it = peerItertors[iteratorid];

                var data = it.Current;
                msg.Params["data"] = data;
            }
            catch (Exception err)
            {
                msg.Params["_error"] = err.Message.ToBytes_UTF8Encode();
            }
            //这个完全可以不要等待呀
            SendToClient(msg);
        }
        public async Task OnSnapshot_IteratorNext(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.iterator.current.back");
            msg.Params["_id"] = id;

            try
            {
                UInt64 iteratorid = BitConverter.ToUInt64(msg.Params["iteratorid"]);
                var it = peerItertors[iteratorid];

                var b = it.MoveNext();
                msg.Params["movenext"] = new byte[] { (byte)(b ? 1 : 0) };
            }
            catch (Exception err)
            {
                msg.Params["_error"] = err.Message.ToBytes_UTF8Encode();
            }
            //这个完全可以不要等待呀
            SendToClient(msg);
        }
        public async Task OnSnapshot_IteratorReset(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.iterator.reset.back");
            msg.Params["_id"] = id;
            try
            {
                UInt64 iteratorid = BitConverter.ToUInt64(msg.Params["iteratorid"]);
                var it = peerItertors[iteratorid];

                it.Reset();
            }
            catch (Exception err)
            {
                msg.Params["_error"] = err.Message.ToBytes_UTF8Encode();
            }
            //这个完全可以不要等待呀
            SendToClient(msg);
        }
        public static void QuickFixHeight(byte[] data, byte[] heightbuf)
        {
            //var v = data[0];
            var tagLength = data[1];
            //var timestamp = BitConverter.ToUInt64(data, 2 + taglength);
            for (var i = 0; i < 8; i++)
            {
                data[tagLength + 2 + i] = heightbuf[i];
            }
            //var timestamp2 = BitConverter.ToUInt64(data, 2 + taglength);
        }
        public static byte[] QuickGetHeight(byte[] data)
        {
            byte[] heightbuf = new byte[8];
            var tagLength = data[1];
            for (var i = 0; i < 8; i++)
            {
                heightbuf[i] = data[tagLength + 2 + i];
            }
            return heightbuf;
        }
        public async Task OnDB_Write(NetMessage msgRecv, byte[] id)
        {
            var msg = NetMessage.Create("_db.write.back");
            msg.Params["_id"] = id;
            try
            {
                var data = msgRecv.Params["writetask"];
                var lastblockhashRecv = msgRecv.Params["lasthash"];

                using (var snap = Program.storage.maindb.UseSnapShot())
                {
                    var blockidlast = BitConverter.GetBytes((UInt64)(snap.DataHeight - 1));

                    var writetask = WriteTask.FromRaw(data);
                    //不够用，还需要block高度
                    var lasthashFind = snap.GetValue(StorageService.tableID_BlockID2Hash, blockidlast).value;
                    if (Helper.BytesEquals(lastblockhashRecv, lasthashFind))
                    {
                        byte[] taskhash = null;
                        byte[] taskheight = null;

                        //数据追加处理
                        Action<WriteTask, byte[], LightDB.IWriteBatch> afterparser = (_task, _data, _wb) =>
                            {
                                taskhash = Helper.Sha256.ComputeHash(_data);
                                taskheight = QuickGetHeight(_data);
                                _wb.Put(StorageService.tableID_BlockID2Hash, taskheight, DBValue.FromValue(DBValue.Type.Bytes, taskhash));
                            };
                        //写入数据
                        Program.storage.maindb.Write(writetask, afterparser);

                        //进表
                        msg.Params["blockheight"] = taskheight;
                        msg.Params["blockhash"] = taskhash;
                    }
                    else
                    {
                        msg.Params["_error"] = "block hash is error".ToBytes_UTF8Encode();
                    }

                }
            }
            catch (Exception err)
            {
                msg.Params["_error"] = err.Message.ToBytes_UTF8Encode();
            }
            //这个完全可以不要等待呀
            SendToClient(msg);
        }
    }
}

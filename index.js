const express = require("express");
const ejs = require("ejs");
const http = require("http");
const path = require("path");
const {Server} = require('socket.io'); //creating a server
const sqlite3 = require("sqlite3");
const {open} = require("sqlite");
const {availableParallelism} = require('node:os');
const cluster = require('node:cluster');
const {createAdapter, setupPrimary} = require('@socket.io/cluster-adapter');

if(cluster.isPrimary){
    const numCPUs = availableParallelism();
    //creating one worker per available core 
    for(let i=0; i<numCPUs; i++){
        cluster.fork({
            PORT : 5050 + i
        });
    }

    //setting up the adapter on the primary thread
    return setupPrimary();
}


async function main(){
    //connecting to database
    const db = await open({
        filename : 'chat.db',
        driver : sqlite3.Database
    });

await db.exec(`
    CREATE TABLE IF NOT EXISTS messages(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_offset TEXT UNIQUE,
        content TEXT
    );
`);

const app =express();
const server = http.createServer(app);
const io = new Server(server, {
    connectionStateRecovery: {},
    //setting up the adapter on each worker thread
    adapter : createAdapter()
});

//middlewares
app.set("views", path.join(__dirname, "views"));
app.set("view engine", "ejs");

app.get('/', (req, res)=>{
    res.render("index.ejs");
})

io.on('connection', async(socket)=>{
    console.log("user connected");
    socket.on('chat message', async(msg, clientOffset, callback)=>{
        console.log('message:', msg);
        let result;
        try{
            result = await db.run('INSERT INTO messages (content) VALUES (?, ?)', msg, clientOffset);
        } catch(e){
            if(e.errno === 19 /*sqlite constraint*/){
                //the message was already inserted
                callback();
            }else{
                //let the client retry
            }
            return;
        }
        console.log("recovered?", socket.recovered);
        io.emit('chat message', msg, result.lastID);
        //acknowledge the event
        callback();

    });
        if(!socket.recovered){
            try{
                await db.each('SELECT id, content FROM messages WHERE id>?', [socket.handshake.auth.serverOffset || 0],
                (_err, row)=>{
                    socket.emit('chat message', row.content, row.id);
                }
                )
            }catch(e){
                console.log(e);
            }
        }
        socket.on('disconnect', ()=>{
        console.log('user disconnected');
    })
}); //initialize a new instance of socket.io by passing the server (the HTTP server) object, then listen on the connection event for incoming sockets and log it to the console.

const port = process.env.PORT;
server.listen(port, () => {
  console.log(`Server listening to port ${port}`);
});
}

main();

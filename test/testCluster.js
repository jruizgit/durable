require("../lib/durable");
var cluster = require("cluster");
var port = parseInt(process.argv[2]);

if (cluster.isMaster) {
  for (var i = 0; i < 4; i++) {
    cluster.fork();
  }

  cluster.on("exit", function(worker, code, signal) {
    cluster.fork();
  });
}
else {
    durable.run({
		
		drive: durable.flowChart({
			start : {
				run: durable.receive({ content: "go" })
					 .continueWith(function(s) { s.setInput({ id: 2, content: "wait"}); })
					 .post(),
				to: "send"
			},
			send: {
				run: durable.promise(function (s) {                        
						if (!s.i) {							
							s.i = 1;
						}
						else {
							s.i = s.i + 1;							
						}
						s.setInput({ promise: "ping", session: s.i, id: 1, content: "a" });							
					})
					.post(),
				   
				to: {
					send: function (s) { return (!s.i || s.i < 4) },
					end: function (s) { return (s.i && s.i === 4) }
				}
			},
			end: {
			}
		}),
		
        ping: durable.stateChart({
            start: {
                firstTransition: {
                    whenAll: { 
						a: durable.tryReceive({ content: "a" }),                        
						b: durable.tryCondition(function (s) { return (!s.i || s.i < 500); })
					},
                    run: durable.promise(function (s) {                        
                            if (!s.i) {
								s.i = 2;
                            }
							else {
								s.i = s.i + 1;
							}
                            
							if (s.i % 100 === 0) {
                                console.log("Ping\t" + s.i + "\t" + s.id + "\t" + new Date());
                            }
                            s.setInput({ promise: "pong", session: s.id, id: s.i, content: "b" });							
                        })
                        .post(),
                    to: "start"
                },
                lastTransition: {
                    when: durable.tryCondition(function (s) { return (s.i === 500); }),
                    to: "end"
                }
            },
            end: {
            }
        }),

        pong: durable.stateChart({
            start: {
                firstTransition: {
                    whenAll: { 
						a: durable.tryReceive({ content: "b" }),                        
						b: durable.tryCondition(function (s) { return (!s.i || s.i < 500); })
					},
                    run: durable.promise(function (s) {                        
                            if (!s.i) {
								s.i = 2;
                            }
							else {
								s.i = s.i + 1;
							}
							
                            if (s.i % 100 === 0) {
                                console.log("Pong\t" + s.i + "\t" + s.id + "\t" + new Date());
                            }
                            s.setInput({ promise: "ping", session: s.id, id: s.i, content: "a" });							
                        })
                        .post(),
                    to: "start"
                },
                lastTransition: {
                    when: durable.tryCondition(function (s) { return (s.i === 500); }),
                    to: "end"
                }
            },
            end: {
            }            
        })
    });
}
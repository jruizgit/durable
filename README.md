durable.js
=======
A lightweight, efficient and reliable library for coordinating long lasting event streams

var d = require('durable');

d.run({
  myChart: d.stateChart({
  s1: {
    t1: {
        whenAny: {
          a: d.tryReceive( { content: 'a' } ),
          b: d.tryReceive( { content: 'b' } ) 
        },
        run: function(s) { s.i = s.i + 1 },
        to: "s2" 
      } 
    },
  s2: {
    t1: {
        when: d.tryReceive( { content: 'c'} ),
        run: function(s) { s.i = s.i + 1 },
        to: "s1" 
      },
    t1: {
        when: d.tryReceive( { content: 'd'} ),
        to: "final"
      }
    },
  final: { 
    }
  }
});
    
    

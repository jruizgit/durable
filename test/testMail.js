var d = require('../lib/durable');
require('../lib/durableMail');

durable.run({

    mail: durable.receive()
        .continueWith(function(s) {
            var message = s.getOutput();
            s.to = message.to;
            message.subject = 'Need your approval';
            message.html = 'Go <a href="http://localhost:5000/testMail.html?p=mail&s=' + s.id + '">here</a> to approve or deny.';
            s.setInput(message);
        })
        .postMail()
        .checkpoint('mailSent')
        .receive({ subject: 'approved' })
        .continueWith(function(s) {
            s.setInput({ to: s.to, subject: "Thank you for approving <EOM>"});
        })
        .postMail()
        .checkpoint('done'),

    $appExtension: function(app) {
        var stat = require('node-static');
        var fileServer = new stat.Server(__dirname);

        app.get('/testMail.html', function (request, response) {
            request.addListener('end', function () {
                fileServer.serveFile('/testMail.html', 200, {}, request, response);
            }).resume();
        });
     }
}, null, null, 'examples');

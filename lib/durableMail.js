exports = module.exports = durableMail = function () {
    var simplesmtp = require('simplesmtp');
    var MailComposer = require('mailcomposer').MailComposer;
    var durable = require('./durable');

    var postMail = function (message) {

        var execute = function (session) {
            message = session.getInput() || message;
            var options = {
                host: 'smtp.gmail.com',
                secureConnection: true,
                requiresAuth: true,
                domains: ['gmail.com', 'googlemail.com'],
                authMethod: 'PLAIN',
                maxConnections: 5
            };
            if (!message.from) {
                message.from = 'durablejs@gmail.com';
                options.auth = {
                    user: 'durablejs@gmail.com',
                    pass: 'durablepass'
                };
            }
            else {
                options.auth = {
                    user: message.userName,
                    pass: message.password
                };

                delete (message.username);
                delete (message.password);
            }

            session.addSideEffect(function (session, complete) {
                var pool = simplesmtp.createClientPool(465, options.host, options);
                var composer = new MailComposer();
                composer.addHeader('Date', new Date().toUTCString());
                composer.addHeader('Message-Id', '<' + Date.now() + Math.random().toString(16).substr(1) + '@durablejs>');
                composer.setMessageOption(message);
                pool.sendMail(composer, function (err) {
                    complete(err);
                });
            });
        }

        var that = durable.promise(execute);
        that.type = "postMail";
        that.params = { message: message };
        return that;
    }

    durable.addPromiseExtension('postMail', function (message) {
        return that.continueWith(postMail(message));
    });

    return {
        postMail: postMail
    }
}();
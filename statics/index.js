function XmlR () {
    var xmlR = new XMLHttpRequest();

    xmlR._then = function (response) {
        return xmlR;
    };

    xmlR.then = function (callback) {
        xmlR._then = callback;
        return xmlR;
    };

    xmlR._catch = function (error, statusText, status) {
        return xmlR;
    }
    xmlR.catch = function (callback) {
        xmlR._catch = callback;
        return xmlR;
    };

    xmlR.onreadystatechange = function () {
        if (xmlR.readyState === 4) {
            if (xmlR.status === 200) {
                xmlR._then(xmlR.json && JSON.parse(xmlR.response) || xmlR.response);

            } else {
                xmlR.catch(null, xmlR.statusText, xmlR.status);
            }
            xmlR.json = false;
        }
    }

    xmlR.get = function (url) {
        try {
            xmlR.open('GET', url);
            xmlR.send();
        } catch (error) {
            xmlR.catch(error);
        }
        return xmlR;
    }

    xmlR.post = function (url, data) {
        try {
            xmlR.open('POST', url);
            xmlR.send(JSON.stringify(data));
        } catch (error) {
            xmlR.catch(error);
        }
        return xmlR;
    }

    return xmlR;
}

(function () {
    var xmlR = new XmlR();

    function startFn (e) {
        if (!e.srcElement.classList.contains('active')) {
            return;
        }

        xmlR.json = true;
        xmlR.get('spiders/' + e.srcElement.getAttribute('spiderName') + '/start')
            .then(function (res) {
                if (res.success) {
                    location.reload();
                }
            }).catch(function (error, statusText, status) {
                console.error(error, statusText, status);
            }).json = true;
    }

    function stopFn (e) {

        if (!e.srcElement.classList.contains('active')) {
            return;
        }

        xmlR.json = true;
        xmlR.get('spiders/' + e.srcElement.getAttribute('spiderName') + '/stop')
            .then(function (res) {
                if (res.success) {
                    location.reload();
                }
            }).catch(function (error, statusText, status) {
                console.error(error, statusText, status);
            }).json = true;

    }

    function getFileFn (e) {

        if (!e.srcElement.classList.contains('active')) {
            return;
        }

        xmlR.get('spiders/' + e.srcElement.getAttribute('spiderName') + '/get_file')
            .then(function (response) {
                var blob = new Blob([response], {type: 'text/plain'});
                var url = URL.createObjectURL(blob);
                var a = document.createElement("a");
                a.href = url;
                a.download = e.srcElement.getAttribute('spiderName') + '.csv';
                document.body.appendChild(a);
                a.click();
                setTimeout(function() {
                    document.body.removeChild(a);
                    window.URL.revokeObjectURL(url);
                }, 0);
            }).catch(function (error, statusText, status) {
                console.error(error, statusText, status);
            });
    }

    callbacks = {
        'start': startFn,
        'stop': stopFn,
        'getFile': getFileFn
    };

    document.addEventListener('DOMContentLoaded', function () {
        Array.apply(null, document.getElementsByClassName('btn')).map(function (btn) {
            btn.addEventListener('click', callbacks[btn.classList[1]]);
        });
    });
})();
var elasticsearch = require('elasticsearch');
var mysql = require('mysql');
require('dotenv').config();

var client = new elasticsearch.Client({
    host: process.env.ES_HOST,
    log: 'trace', // <- verbose method (DEVELOPMENT ONLY!)
});

var dataKeywordFormat = function(res) {
    return data = {
        "index": "keyword",
        "type": "_doc",
        "id": res.input_id,
        "body": {
            "keyword": res.keyword,
            "suggestions": res.suggestions
        }
    }
}

var dataTagFormat = function(res) {
    object_size = function(obj) {
        var size = 0,
            key;
        for (key in obj) {
            if (obj.hasOwnProperty(key)) size++;
        }
        return size;
    };

    // Manipulate gender
    var genderData = JSON.parse(res.gender);
    var gender = []

    if (genderData != null) {
        if (object_size(genderData) > 2) {
            gender = ['campur', 'putri', 'putra'];
        } else {
            genderData.forEach(function(val) {
                switch (gender) {
                    case '0':
                        gender.push('campur');
                        break;
                    case '1':
                        gender.push('putri');
                        break;
                    case '2':
                        gender.push('putra');
                        break;
                    default:
                        break;
                }
            })
        }
    }

    return data = {
        "index": "tag",
        "type": "_doc",
        "id": res.id,
        "body": {
            "tags": res.tags,
            "slug": res.slug,
            "keyword": res.keyword,
            "latitude_1": res.latitude_1,
            "longitude_1": res.longitude_1,
            "latitude_2": res.latitude_2,
            "longitude_2": res.longitude_2,
            "type": res.type,
            "area_city": res.area_city,
            "area_subdistrict": res.area_subdistrict,
            "price_min": res.price_min,
            "price_max": res.price_max,
            "gender": gender,
            "rent_type": res.rent_type
        }
    }
}

var dataRoomFormat = function(res) {
    // Manipulate 'completion' fields value
    var name = (res.name == null) ? '' : res.name;
    var address = (res.address == null) ? '' : res.address;
    var area_city = (res.area_city == null) ? '' : res.area_city;
    var area_subdistrict = (res.area_subdistrict == null) ? '' : res.area_subdistrict;
    var area_big = (res.area_big == null) ? '' : res.area_big;

    // Manipulate 'gender' value
    switch (res.gender) {
        case '0':
            var gender = 'campur';
            break;
        case '1':
            var gender = 'putri';
            break;
        case '2':
            var gender = 'putra';
            break;
        default:
            break;
    }

    return data = {
        "index": "room",
        "type": "_doc",
        "id": res.id,
        "body": {
            "name": name,
            "address": address,
            "area_subdistrict": area_subdistrict,
            "area_city": area_city,
            "area_big": area_big,
            "location_id": res.location_id,
            "latitude": res.latitude,
            "longitude": res.longitude,
            "gender": gender,
            "slug": res.slug
        }
    }
}

var startIndexing = function(index_name) {
    var pool = mysql.createPool({
        host: process.env.DB_HOST,
        user: process.env.DB_USER,
        password: process.env.DB_PASS,
        database: process.env.DB_DATABASE
    });

    if (index_name == 'room') {
        var table = process.env.ROOM_TABLE + ' a';
        var select = "SELECT * FROM ";
        var where = ' WHERE a.deleted_at IS NULL';
    } else if (index_name == 'tag') {
        var table = process.env.LANDING_TABLE + ' a';
        var select = "SELECT * FROM ";
        var where = ' LEFT JOIN landing b ON b.id = a.landing_id LEFT JOIN tagging c ON c.id = a.tagging_id WHERE a.deleted_at IS NULL';
    } else { // <- keyword
        var table = process.env.KEYWORD_TABLE + ' a';
        var select = "SELECT a.input_id, b.keyword, a.suggestion, a.area, a.latitude, a.longitude FROM ";
        var where = ' LEFT JOIN input_keyword b on b.id = a.input_id WHERE b.deleted_at IS NULL';
    }

    var countQuery = "SELECT count(*) as total FROM " + table + where;
    var chunkSize = process.env.CHUNK_SIZE;

    pool.getConnection(function(err, connection) {
        if (err) {
            connection.release();
            console.log("Error on getConnection:", err);
            return;
        }

        connection.query(countQuery, {}, function(err, result) {
            if (err) {
                connection.release();
                console.log("Error on getConnection:", err);
                return;
            }

            if (result && result[0]) {
                var totalRows = result[0]['total'];
                console.log("Total rows in DB:", totalRows);
                var periods = Math.ceil(totalRows / chunkSize)
                console.log("Total chunks:", periods);

                var selectQuery = select + table + where + " ORDER BY a.id DESC LIMIT ";

                var counter = 1;

                for (var i = 0; i < periods; i++) {
                    var offset = i * chunkSize;
                    var runQuery = selectQuery + offset + "," + chunkSize;

                    connection.query(runQuery, {}, function(err, results) {
                        if (err) {
                            console.log("Error on runQuery:", err);
                            return;
                        }

                        if (index_name == 'room') {
                            var stopped = false;
                            for (var j = 0; j < results.length; j++) {
                                if (!stopped) {
                                    client.index(dataRoomFormat(results[j]), function(err, resp, status) {
                                        if (err) {
                                            console.log('Indexing error:', status, err);
                                            stopped = true;
                                        } else {
                                            console.log('Indexing ' + j + ': OK');
                                        }
                                    });
                                }
                            }

                        } else if (index_name == 'tag') {
                            var tempData = []; // <- for landing_id grouping
                            for (var j = 0; j < results.length; j++) {
                                var $id = results[j].landing_id;

                                // group by 'landing_id'
                                if (tempData[$id] == undefined) {
                                    tempData[$id] = results[j];
                                }

                                // compile taggings
                                if (tempData[$id].tags == undefined) {
                                    tempData[$id].tags = [];
                                }

                                tempData[$id].tags.push(results[j].name);
                            }

                            // start indexing
                            var stopped = false;
                            tempData.forEach(function(dt) {
                                if (!stopped) {
                                    client.index(dataTagFormat(dt), function(err, resp, status) {
                                        if (err) {
                                            console.log('Indexing error:', status, err);
                                            stopped = true;
                                        } else {
                                            console.log('Indexing status: OK');
                                        }
                                    });
                                }
                            });

                        } else { // << index_name == 'keyword'
                            var tempData = []; // <- for input_id grouping
                            for (var j = 0; j < results.length; j++) {
                                var $id = results[j].input_id;

                                // group by 'input_id'
                                if (tempData[$id] == undefined) {
                                    tempData[$id] = results[j];
                                }

                                // compile suggestions
                                if (tempData[$id].suggestions == undefined) {
                                    tempData[$id].suggestions = [];
                                }

                                tempData[$id].suggestions.push({
                                    'suggestion': results[j].suggestion,
                                    'area': results[j].area,
                                    'latitude': results[j].latitude,
                                    'longitude': results[j].longitude
                                });
                            }

                            // start indexing
                            var stopped = false;
                            tempData.forEach(function(dt) {
                                // console.log(dataKeywordFormat(dt));
                                if (!stopped) {
                                    client.index(dataKeywordFormat(dt), function(err, resp, status) {
                                        if (err) {
                                            console.log('Indexing error:', status, err);
                                            stopped = true;
                                        } else {
                                            console.log('Indexing status: OK');
                                        }
                                    });
                                }
                            });
                        }
                    });

                    counter++;
                }

                connection.release();
            }

        });
    });

    // return indexedData;
}

// Main executions
console.log("Type 'keyword', 'room' or 'tag' to start indexing.");
var stdin = process.openStdin();
stdin.addListener("data", function(d) {
    var type = d.toString().trim();
    if (type == 'keyword' || type == 'room' || type == 'tag') {
        console.log("Begin indexing: " + type + "...");
        // createIndex('test');
        startIndexing(type);
    } else {
        console.log("Type 'keyword', 'room' or 'tag' to start indexing.");
    }
});

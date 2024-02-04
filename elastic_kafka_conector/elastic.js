const { Client } = require("@elastic/elasticsearch");

const ELASTIC_API_KEY =
    "SDQ3MVlJMEJMUS1UMlN0TU5aWGo6X1B4RkdFYzFRXzZKMXNxTlhjNWltUQ==";
const nodeUrl = "https://bsocial.es.europe-west3.gcp.cloud.es.io";

const client = new Client({
    node: nodeUrl, // Elasticsearch endpoint
    auth: {
        apiKey: ELASTIC_API_KEY,
    },
});

// Function to push data to Elasticsearch
async function pushToElasticsearch(data) {
    try {
        console.log("Usli smo");
        const response = await client.index(data);
    } catch (error) {
        console.error("Error indexing data:", error);
    }
}

//Taking number of logged users

async function countLoggedInPlayersPerDay() {
    try {
        const result = await client.search({
            index: "topic_login",
            body: {
                size: 0,
                aggs: {
                    login_counts_per_day: {
                        date_histogram: {
                            field: "registrationDate",
                            calendar_interval: "day",
                            format: "yyyy-MM-dd",
                            min_doc_count: 0,
                        },
                    },
                },
            },
        });
        const countsPerDay =
            result.aggregations.login_counts_per_day.buckets.map((bucket) => {
                return {
                    date: bucket.key_as_string,
                    count: bucket.doc_count,
                };
            });
        console.log(countsPerDay);
        return countsPerDay;
    } catch (error) {
        console.error("An error occurred while querying Elasticsearch:", error);
    }
}

async function getTop10Posts() {
    try {
        const result = await client.search({
            index: "topic_post",
            body: {
                size: 0,
                aggs: {
                    posts_per_day: {
                        date_histogram: {
                            field: "timestamp",
                            calendar_interval: "day",
                            format: "yyyy-MM-dd",
                            min_doc_count: 0,
                            extended_bounds: {
                                min: "now-10d/d",
                                max: "now/d",
                            },
                        },
                        aggs: {
                            top_posts: {
                                terms: {
                                    field: "postId",
                                    size: 10,
                                    order: {
                                        comments_count: "desc",
                                    },
                                },
                                aggs: {
                                    comments_count: {
                                        value_count: {
                                            field: "commentId",
                                        },
                                    },
                                    post_data: {
                                        top_hits: {
                                            size: 1,
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
            },
        });

        const topPostsPerDay = result.aggregations.posts_per_day.buckets.map(
            (bucket) => {
                return {
                    date: bucket.key_as_string,
                    topPosts: bucket.top_posts.buckets.map((post) => {
                        const postData = post.post_data.hits.hits[0]._source;
                        return {
                            postId: post.key,
                            commentsCount: post.comments_count.value,
                            messageContent: postData.messageContent,
                        };
                    }),
                };
            }
        );

        return topPostsPerDay;
    } catch (error) {
        console.error("An error occurred while querying Elasticsearch:", error);
        return [];
    }
}

async function getLatestPostsForUsers() {
    try {
        const result = await client.search({
            index: "topic_post",
            body: {
                size: 0,
                aggs: {
                    latest_posts: {
                        terms: {
                            field: "username.keyword",
                            size: 10,
                        },
                        aggs: {
                            latest_post: {
                                top_hits: {
                                    size: 1,
                                    sort: [{ timestamp: "desc" }],
                                },
                            },
                        },
                    },
                },
            },
        });

        const latestPostsForUsers =
            result.aggregations.latest_posts.buckets.map((bucket) => {
                const latestPost = bucket.latest_post.hits.hits[0]._source;
                return {
                    username: bucket.key,
                    latestPost: {
                        postId: latestPost.postId,
                        messageContent: latestPost.messageContent,
                        timestamp: latestPost.timestamp,
                    },
                };
            });

        return latestPostsForUsers;
    } catch (error) {
        console.error("An error occurred while querying Elasticsearch:", error);
        return [];
    }
}

module.exports = {
    pushToElasticsearch,
    countLoggedInPlayersPerDay,
    getTop10Posts,
    getLatestPostsForUsers,
};

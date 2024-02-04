const { runKafkaConsumer } = require("./kafka");
const {
    latest_post,
    countLoggedInPlayersPerDay,
    getTop10Posts,
    getLatestPostsForUsers,
} = require("./elastic");


async function main() {
    await runKafkaConsumer();
}

main();


countLoggedInPlayersPerDay();

async function printTopPostsPerDay() {
    try {
        const topPostsPerDay = await getTop10Posts();

        topPostsPerDay.forEach((day) => {
            console.log(`Date: ${day.date}`);
            if (day.topPosts.length === 0) {
                console.log("No top posts for this day.");
            } else {
                console.log("Top posts for this day:");
                day.topPosts.forEach((post, index) => {
                    console.log(`Post ${index + 1}:`);
                    console.log(`  Post ID: ${post.postId}`);
                    console.log(`  Message Content: ${post.messageContent}`);
                });
            }
            console.log();
        });
    } catch (error) {
        console.error("An error occurred:", error);
    }
}

printTopPostsPerDay();

async function second() {
    try {
        // Calling latest posts users
        const latestPostsForUsers = await getLatestPostsForUsers();

        // Showing resuts
        latestPostsForUsers.forEach((user) => {
            console.log(`Latest post for user ${user.username}:`);
            console.log(`Post ID: ${user.latestPost.postId}`);
            console.log(`Message Content: ${user.latestPost.messageContent}`);
            console.log(`Timestamp: ${user.latestPost.timestamp}`);
            console.log();
        });
    } catch (error) {
        console.error("An error occurred:", error);
    }
}

second();

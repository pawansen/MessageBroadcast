const mongoose = require('mongoose');
const commentsSchema = new mongoose.Schema({
    postId: Number,
    id: Number,
    name: String,
    email: String,
    body: String,
})

module.exports = mongoose.model('comments', commentsSchema);

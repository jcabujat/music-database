package com.jcabujat.model;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DataSource {
    public static final String DB_NAME = "music.db";

    public static final String CONNECTION_STRING = "jdbc:sqlite:C:\\Users\\jonat\\IdeaProjects\\Music\\" + DB_NAME;

    public static final String TABLE_ALBUMS = "albums";
    public static final String COLUMN_ALBUM_ID = "_id";
    public static final String COLUMN_ALBUM_NAME = "name";
    public static final String COLUMN_ALBUM_ARTIST = "artist";
    public static final int INDEX_ALBUM_ID = 1;
    public static final int INDEX_ALBUM_NAME = 2;
    public static final int INDEX_ALBUM_ARTIST = 3;

    public static final String TABLE_ARTISTS = "artists";
    public static final String COLUMN_ARTIST_ID = "_id";
    public static final String COLUMN_ARTIST_NAME = "name";
    public static final int INDEX_ARTIST_ID = 1;
    public static final int INDEX_ARTIST_NAME = 2;


    public static final String TABLE_SONGS = "songs";
    public static final String COLUMN_SONG_ID = "_id";
    public static final String COLUMN_SONG_TRACK = "track";
    public static final String COLUMN_SONG_TITLE = "title";
    public static final String COLUMN_SONG_ALBUM = "album";
    public static final int INDEX_SONG_ID = 1;
    public static final int INDEX_SONG_TRACK = 2;
    public static final int INDEX_SONG_TITLE = 3;
    public static final int INDEX_SONG_ALBUM = 4;

    public enum ORDER_BY {
        NONE, ASC, DESC
    }

    public static final String QUERY_ALBUMS_BY_ARTIST_START = "SELECT " +
            TABLE_ALBUMS + '.' + COLUMN_ALBUM_NAME + " FROM " + TABLE_ALBUMS + " INNER JOIN " + TABLE_ARTISTS + " ON " +
            TABLE_ALBUMS + '.' + COLUMN_ALBUM_ARTIST + " = " + TABLE_ARTISTS + '.' + COLUMN_ARTIST_ID + " WHERE " +
            TABLE_ARTISTS + '.' + COLUMN_ARTIST_NAME + " = \"";

//    SELECT artists.name, albums.name, songs.track FROM songs
//    INNER JOIN albums ON songs.album = albums._id
//    INNER JOIN artists ON albums.artist = artists._id
//    WHERE songs.title = "Go Your Own Way"

    public static final String QUERY_ARTIST_BY_SONG_START = "SELECT " + TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + ", " +
            TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + ", " + TABLE_SONGS + "." + COLUMN_SONG_TRACK + " FROM " +
            TABLE_SONGS + " INNER JOIN " + TABLE_ALBUMS + " ON " + TABLE_SONGS + "." + COLUMN_SONG_ALBUM + " = " +
            TABLE_ALBUMS + "." + COLUMN_ALBUM_ID + " INNER JOIN " + TABLE_ARTISTS + " ON " + TABLE_ALBUMS + "." +
            COLUMN_ALBUM_ARTIST + " = " + TABLE_ARTISTS + "." + COLUMN_ARTIST_ID + " WHERE " + TABLE_SONGS + "." +
            COLUMN_SONG_TITLE + " = \"";

    public static final String VIEW_ARTIST_SONG = "artist_song";

    // CREATE VIEW IF NOT EXISTS artist_song AS SELECT artists.name, albums.name AS album,
    // songs.track, songs.title FROM songs INNER JOIN albums ON songs.album = albums._id
    // INNER JOIN artists ON albums.artist = artists._id ORDER BY artists.name, albums.name, songs.track

    public static final String CREATE_ARTIST_SONG_VIEW = "CREATE VIEW IF NOT EXISTS " + VIEW_ARTIST_SONG +
            " AS SELECT " + TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + ", " + TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + " AS " +
            COLUMN_SONG_ALBUM + ", " + TABLE_SONGS + "." + COLUMN_SONG_TRACK + ", " + TABLE_SONGS + "." +
            COLUMN_SONG_TITLE + " FROM " + TABLE_SONGS + " INNER JOIN " + TABLE_ALBUMS + " ON " + TABLE_SONGS + "." +
            COLUMN_SONG_ALBUM + " = " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ID + " INNER JOIN " + TABLE_ARTISTS + " ON " +
            TABLE_ALBUMS + "." + COLUMN_ALBUM_ARTIST + " = " + TABLE_ARTISTS + "." + COLUMN_ARTIST_ID + " ORDER BY " +
            TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + ", " + TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + ", " +
            TABLE_SONGS + "." + COLUMN_SONG_TRACK;

//    SELECT name, album, track FROM artist_song
//    WHERE title = "Go Your Own Way"

    public static final String QUERY_SONG_INFO = "SELECT " + COLUMN_ARTIST_NAME + ", " + COLUMN_SONG_ALBUM + ", " +
            COLUMN_SONG_TRACK + " FROM " + VIEW_ARTIST_SONG + " WHERE " + COLUMN_SONG_TITLE + " = \"";

    public static final String QUERY_SONG_INFO_PREP = "SELECT " + COLUMN_ARTIST_NAME + ", " + COLUMN_SONG_ALBUM + ", " +
            COLUMN_SONG_TRACK + " FROM " + VIEW_ARTIST_SONG + " WHERE " + COLUMN_SONG_TITLE + " = ?";

    public static final String INSERT_ARTIST = "INSERT INTO " + TABLE_ARTISTS +
            "(" + COLUMN_ARTIST_NAME + ") VALUES(?)";
    public static final String INSERT_ALBUM = "INSERT INTO " + TABLE_ALBUMS +
            "(" + COLUMN_ALBUM_NAME + ", " + COLUMN_ALBUM_ARTIST + ") VALUES(?, ?)";
    public static final String INSERT_SONG = "INSERT INTO " + TABLE_SONGS +
            "(" + COLUMN_SONG_TRACK + ", " + COLUMN_SONG_TITLE + ", " + COLUMN_SONG_ALBUM +
            ") VALUES(?, ?, ?)";

    public static final String QUERY_ARTIST = "SELECT " + COLUMN_ARTIST_ID + " FROM " +
            TABLE_ARTISTS + " WHERE " + COLUMN_ARTIST_NAME + " = ?";
    public static final String QUERY_ALBUM = "SELECT " + COLUMN_ALBUM_ID + " FROM " +
            TABLE_ALBUMS + " WHERE " + COLUMN_ALBUM_NAME + " = ?";


    private Connection conn;
    private PreparedStatement querySongInfo;
    private PreparedStatement insertIntoArtists;
    private PreparedStatement insertIntoAlbums;
    private PreparedStatement insertIntoSongs;
    private PreparedStatement queryArtist;
    private PreparedStatement queryAlbum;

    public boolean open() {
        try {
            conn = DriverManager.getConnection(CONNECTION_STRING);
            querySongInfo = conn.prepareStatement(QUERY_SONG_INFO_PREP);
            insertIntoArtists = conn.prepareStatement(INSERT_ARTIST, Statement.RETURN_GENERATED_KEYS);
            insertIntoAlbums = conn.prepareStatement(INSERT_ALBUM, Statement.RETURN_GENERATED_KEYS);
            insertIntoSongs = conn.prepareStatement(INSERT_SONG);
            queryArtist = conn.prepareStatement(QUERY_ARTIST);
            queryAlbum = conn.prepareStatement(QUERY_ALBUM);
            return true;
        } catch (SQLException e) {
            System.out.println("Couldn't connect to database: " + e.getMessage());
            return false;
        }
    }

    public void close() {
        try {
            if (conn != null) {
                querySongInfo.close();
                insertIntoArtists.close();
                insertIntoAlbums.close();
                insertIntoSongs.close();
                queryArtist.close();
                queryAlbum.close();
                conn.close();
            }
        } catch (SQLException e) {
            System.out.println("Couldn't close the connection: " + e.getMessage());
        }
    }

    public List<Artist> queryArtists(ORDER_BY order) {
//    public List<Artist> queryArtists(int sortOrder) {

        StringBuilder sb = new StringBuilder("SELECT * FROM ");
        sb.append(TABLE_ARTISTS);
        if (order != ORDER_BY.NONE) {
//        if (sortOrder != ORDER_BY_NONE) {
            sb.append(" ORDER BY ");
            sb.append(COLUMN_ARTIST_NAME);
            sb.append(" COLLATE NOCASE ");
            if (order == ORDER_BY.DESC) {
//            if (sortOrder == ORDER_BY_DESC) {
                sb.append("DESC");
            } else {
                sb.append("ASC");
            }
        }

        try (Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery(sb.toString())) {

            List<Artist> artistList = new ArrayList<>();
            while (resultSet.next()) {
                Artist artist = new Artist();
                artist.setId(resultSet.getInt(INDEX_ARTIST_ID));
                artist.setName(resultSet.getString(INDEX_ARTIST_NAME));
                artistList.add(artist);
            }

            return artistList;

        } catch (SQLException e) {
            System.out.println("Artist query failed: " + e.getMessage());
            return null;
        }
    }

    public List<String> queryAlbumsForArtist(String artistName, ORDER_BY order) {

//        SELECT albums.name FROM albums
//        INNER JOIN artists ON albums.artist = artists._id
//        WHERE artists.name = "Carole King"
//        ORDER BY albums.name COLLATE NOCASE ASC

        StringBuilder sb = new StringBuilder(QUERY_ALBUMS_BY_ARTIST_START);
        sb.append(artistName);
        sb.append("\"");

        if (order != ORDER_BY.NONE) {
//        if (sortOrder != ORDER_BY_NONE) {
            sb.append(" ORDER BY ");
            sb.append(TABLE_ALBUMS);
            sb.append('.');
            sb.append(COLUMN_ALBUM_NAME);
            sb.append(" COLLATE NOCASE ");
            if (order == ORDER_BY.DESC) {
//            if (sortOrder == ORDER_BY_DESC) {
                sb.append("DESC");
            } else {
                sb.append("ASC");
            }
        }

        System.out.println("SQL statement: " + sb.toString());

        try (Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery(sb.toString())) {

            List<String> albums = new ArrayList<>();
            while (resultSet.next()) {
                albums.add(resultSet.getString(1));
            }

            return albums;

        } catch (SQLException e) {
            System.out.println("Query albums by artist failed: " + e.getMessage());
            return null;
        }

    }

    public List<SongArtist> queryArtistsBySong(String song, ORDER_BY order) {
        StringBuilder sb = new StringBuilder(QUERY_ARTIST_BY_SONG_START);
        sb.append(song);
        sb.append("\"");

        //    ORDER BY artists.name, albums.name COLLATE NOCASE ASC
        if (order != ORDER_BY.NONE) {
//        if (sortOrder != ORDER_BY_NONE) {
            sb.append(" ORDER BY ");
            sb.append(TABLE_ARTISTS);
            sb.append('.');
            sb.append(COLUMN_ARTIST_NAME);
            sb.append(", ");
            sb.append(TABLE_ALBUMS);
            sb.append(".");
            sb.append(COLUMN_ALBUM_NAME);
            sb.append(" COLLATE NOCASE ");
            if (order == ORDER_BY.DESC) {
//            if (sortOrder == ORDER_BY_DESC) {
                sb.append("DESC");
            } else {
                sb.append("ASC");
            }
        }

        System.out.println(sb.toString());

        try (Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery(sb.toString())) {

            List<SongArtist> songArtists = new ArrayList<>();
            while (resultSet.next()) {
                SongArtist songArtist = new SongArtist();
                songArtist.setArtistName(resultSet.getString(1));
                songArtist.setAlbumName(resultSet.getString(2));
                songArtist.setSongTrack(resultSet.getInt(3));
                songArtists.add(songArtist);
            }

            return songArtists;

        } catch (SQLException e) {
            System.out.println("Query albums by artist failed: " + e.getMessage());
            return null;
        }

    }

    public int getRecordCount(String table) {
        String sql = "SELECT COUNT(*) FROM " + table;

        try (Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            int count = resultSet.getInt(1);
            return count;
        } catch (SQLException e) {
            System.out.println("Error getting record count: " + e.getMessage());
            return -1;
        }
    }

    public boolean createArtistSongView() {
        try (Statement statement = conn.createStatement()) {
            statement.execute(CREATE_ARTIST_SONG_VIEW);
            return true;
        } catch (SQLException e) {
            System.out.println("Unable to create view: " + e.getMessage());
            return false;
        }
    }

    public List<SongArtist> querySongInfoView(String song) {

        try {
            querySongInfo.setString(1, song);
            ResultSet resultSet = querySongInfo.executeQuery();

            List<SongArtist> songArtists = new ArrayList<>();
            while (resultSet.next()) {
                SongArtist songArtist = new SongArtist();
                songArtist.setArtistName(resultSet.getString(1));
                songArtist.setAlbumName(resultSet.getString(2));
                songArtist.setSongTrack(resultSet.getInt(3));
                songArtists.add(songArtist);
            }

            return songArtists;

        } catch (SQLException e) {
            System.out.println("Query song view failed: " + e.getMessage());
            return null;
        }
    }

    private int insertArtist(String name) throws SQLException {
        queryArtist.setString(1, name);
        ResultSet resultSet = queryArtist.executeQuery();
        if (resultSet.next()) {
            return resultSet.getInt(1);
        } else {
            // Insert the artist
            insertIntoArtists.setString(1, name);
            int affectedRows = insertIntoArtists.executeUpdate();

            if (affectedRows != 1) {
                throw new SQLException("Couldn't insert artist " + name);
            }

            ResultSet generatedKey = insertIntoArtists.getGeneratedKeys();
            if (generatedKey.next()) {
                return generatedKey.getInt(1);
            } else {
                throw new SQLException("Couldn't get id for inserted artist.");
            }
        }
    }

    private int insertAlbum(String name, int artistId) throws SQLException {
        queryAlbum.setString(1, name);
        ResultSet resultSet = queryAlbum.executeQuery();
        if (resultSet.next()) {
            return resultSet.getInt(1);
        } else {
            // Insert the album
            insertIntoAlbums.setString(1, name);
            insertIntoAlbums.setInt(2, artistId);
            int affectedRows = insertIntoAlbums.executeUpdate();

            if (affectedRows != 1) {
                throw new SQLException("Couldn't insert album " + name);
            }

            ResultSet generatedKey = insertIntoAlbums.getGeneratedKeys();
            if (generatedKey.next()) {
                return generatedKey.getInt(1);
            } else {
                throw new SQLException("Couldn't get id for inserted album.");
            }
        }
    }

    public void insertSong(String title, String artist, String album, int track) {

        List<SongArtist> songArtists = querySongInfoView(title);
        boolean songAlreadyExists = false;
        for (SongArtist songArtist : songArtists) {
            if (songArtist.getArtistName().equalsIgnoreCase(artist) &&
                    songArtist.getAlbumName().equalsIgnoreCase(album)) {
                songAlreadyExists = true;
                break;
            }
        }
        if (songAlreadyExists) {
            System.out.println("Song already exists.");
            return;
        }

        try {
            conn.setAutoCommit(false);

            int artistId = insertArtist(artist);
            int albumId = insertAlbum(album, artistId);

            insertIntoSongs.setInt(1, track);
            insertIntoSongs.setString(2, title);
            insertIntoSongs.setInt(3, albumId);

            int affectedRow = insertIntoSongs.executeUpdate();
            if (affectedRow == 1) {
                System.out.println("Song successfully added to the database.");
                conn.commit();
            } else {
                throw new SQLException("Song insertion failed");
            }

        } catch (Exception e) {
            System.out.println("Insert song exception: " + e.getMessage());
            try {
                System.out.println("Performing rollback");
                conn.rollback();
            } catch (SQLException e2) {
                System.out.println("Rollback failed: " + e2.getMessage());
            }
        } finally {
            try {
                System.out.println("Resetting default commit behavior");
                conn.setAutoCommit(true);
            } catch (SQLException e) {
                System.out.println("Couldn't reset auto-commit " + e.getMessage());
            }
        }
    }

}

package com.jcabujat;

import com.jcabujat.model.Artist;
import com.jcabujat.model.DataSource;
import com.jcabujat.model.SongArtist;

import java.util.List;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
        DataSource dataSource = new DataSource();
        if (!dataSource.open()) {
            System.out.println("Can't connect to database.");
            return;
        }

//        List<Artist> artistList = dataSource.queryArtists(DataSource.ORDER_BY.NONE);
//        if (artistList == null) {
//            System.out.println("No artists!");
//            return;
//        }
//
//        for (Artist artist : artistList) {
//            System.out.println("Id = " + artist.getId() + ", Name = " + artist.getName());
//        }

//        List<String> albums = dataSource.queryAlbumsForArtist("Pink Floyd", DataSource.ORDER_BY.DESC);
//        if (albums != null) {
//            for (String album : albums) {
//                System.out.println(album);
//            }
//        }
//
//        List<SongArtist> songArtists = dataSource.queryArtistsBySong("Go Your Own Way", DataSource.ORDER_BY.ASC);
//        if (songArtists != null) {
//            for (SongArtist songArtist : songArtists) {
//                System.out.println(songArtist.getArtistName() + ", " + songArtist.getAlbumName() + ", " + songArtist.getSongTrack());
//            }
//        }
//
//        System.out.println("Record count = " + dataSource.getRecordCount(DataSource.TABLE_ARTISTS));

        dataSource.createArtistSongView();

        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter a song title: ");
        String title = scanner.nextLine();

        List<SongArtist> songArtists = dataSource.querySongInfoView(title);
        if (!songArtists.isEmpty()) {
            for (SongArtist songArtist : songArtists) {
                System.out.println(songArtist.getArtistName() + ", " + songArtist.getAlbumName() + ", " + songArtist.getSongTrack());
            }
        }


        dataSource.close();
    }
}

package mapreduce.genreanalysis;

public class MostPopularGenreByYearMain {
    public static void main(String[] args) throws Exception {
        if(args.length != 2){
            System.err.println("Usage: MostPopularGenreByYearMain <input path> <output path>");
            System.exit(-1);
        }
        //Se define el path de entrada y salida
        String firstInput = args[0];
        String lastInput = args[1];
        String[] firstPaths = new String[2];
        firstPaths[0] = firstInput;
        firstPaths[1] = "outputGenresByYear";

        //Se pasa el path de entrada y salida al método main de la clase GenresByYearMapRed (primer trabajo)
        GenresByYearMapRed.main(firstPaths);
        String[] SecondPaths = new String[2];
        SecondPaths[0] = firstPaths[1];
        SecondPaths[1] = "outputGenreByYearCounter";

        //Se pasa el path de entrada y salida al método main de la clase PopularGenresByYearMapRed (segundo trabajo)
        PopularGenresByYear.main(SecondPaths);
        String[] FinalPaths = new String[2];
        FinalPaths[0] = SecondPaths[1];
        FinalPaths[1] = lastInput;

        //Por ultimo, se pasa el path de entrada y salida al método main de la clase SortPopularGenresByYearMapRed (tercer trabajo)
        SortPopularGenresByYearMapRed.main(FinalPaths);
        System.exit(0);
    }
}

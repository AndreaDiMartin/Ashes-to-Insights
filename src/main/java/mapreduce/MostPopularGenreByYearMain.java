package mapreduce;

public class MostPopularGenreByYearMain {
    public static void main(String[] args) throws Exception {
        if(args.length != 2){
            System.err.println("Usage: MostPopularGenreByYearMain <input path> <output path>");
            System.exit(-1);
        }
        String firstInput = args[0];
        String lastInput = args[1];
        String[] firstPaths = new String[2];
        firstPaths[0] = firstInput;
        firstPaths[1] = "outputGenresByYear";
        GenresByYearMapRed.main(firstPaths);
        String[] SecondPaths = new String[2];
        SecondPaths[0] = firstPaths[1];
        SecondPaths[1] = "outputGenreByYearCounter";
        PopularGenresByYear.main(SecondPaths);
        String[] FinalPaths = new String[2];
        FinalPaths[0] = SecondPaths[1];
        FinalPaths[1] = lastInput;
        SortPopularGenresByYearMapRed.main(FinalPaths);
    }
}

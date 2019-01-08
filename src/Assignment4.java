import java.io.*;
import java.sql.*;
import java.util.Calendar;
import java.text.SimpleDateFormat;
import javafx.util.Pair;
import java.util.ArrayList;
import java.time.YearMonth;
import java.time.LocalDate;


public class Assignment4 {

    private Connection connection = null;
    private final String username = "";
    private final String password = "";
    private final String connectionUrl = "jdbc:sqlserver://localhost;integratedSecurity=false";
    private final String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";


    private Assignment4() {
        Connector();
    }

    public static void executeFunc(Assignment4 ass, String[] args) {
        String funcName = args[0];
        switch (funcName) {
            case "loadNeighborhoodsFromCsv":
                ass.loadNeighborhoodsFromCsv(args[1]);
                break;
            case "dropDB":
                ass.dropDB();
                break;
            case "initDB":
                ass.initDB(args[1]);
                break;
            case "updateEmployeeSalaries":
                ass.updateEmployeeSalaries(Double.parseDouble(args[1]));
                break;
            case "getEmployeeTotalSalary":
                System.out.println(ass.getEmployeeTotalSalary());
                break;
            case "updateAllProjectsBudget":
                ass.updateAllProjectsBudget(Double.parseDouble(args[1]));
                break;
            case "getTotalProjectBudget":
                System.out.println(ass.getTotalProjectBudget());
                break;
            case "calculateIncomeFromParking":
                System.out.println(ass.calculateIncomeFromParking(Integer.parseInt(args[1])));
                break;
            case "getMostProfitableParkingAreas":
                System.out.println(ass.getMostProfitableParkingAreas());
                break;
            case "getNumberOfParkingByArea":
                System.out.println(ass.getNumberOfParkingByArea());
                break;
            case "getNumberOfDistinctCarsByArea":
                System.out.println(ass.getNumberOfDistinctCarsByArea());
                break;
            case "AddEmployee":
                SimpleDateFormat format = new SimpleDateFormat("dd-MM-yyyy");
                ass.AddEmployee(Integer.parseInt(args[1]), args[2], args[3], java.sql.Date.valueOf(args[4]), args[5], Integer.parseInt(args[6]), Integer.parseInt(args[7]), args[8]);
                break;
            default:
                break;
        }
    }



    public static void main(String[] args) {
        File file = new File(".");
        String csvFile = args[0];
        String line = "";
        String cvsSplitBy = ",";
        Assignment4 ass = new Assignment4();
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            while ((line = br.readLine()) != null) {
                String[] row = line.split(cvsSplitBy);
                executeFunc(ass, row);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public void Connector(){
        try {
            Class.forName(this.driver);
            this.connection = DriverManager.getConnection(this.connectionUrl,this.username,this.password);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void Disconnector(){
        try {
            this.connection.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }




    private void loadNeighborhoodsFromCsv(String csvPath) {
        String cvsSplitBy = ",";
        String line;
        if(this.connection==null){
            Connector();
        }
        String sql = "INSERT INTO Neighborhood(NID,Name) VALUES(?,?)";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            try (BufferedReader br = new BufferedReader(new FileReader(csvPath))) {
                while ((line = br.readLine()) != null) {
                    String[] row = line.split(cvsSplitBy);
                    pstmt.setInt(1,Integer.parseInt(row[0]));
                    pstmt.setString(2,row[1]);
                    pstmt.executeUpdate();
                    connection.commit();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        Disconnector();
    }

    private void updateEmployeeSalaries(double percentage) {
        if(this.connection==null){
            Connector();
        }
        try{
            Statement stmt = connection.createStatement(
                    ResultSet.TYPE_SCROLL_SENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            stmt.executeQuery("SELECT ce.* FROM [ConstructorEmployee] ce , [Employee] e WHERE e.EID = ce.EID AND (DATEDIFF(year, e.BirthDate, GETDATE()) > 50)");
            ResultSet results = stmt.getResultSet();
            while (results.next())
            {
                double tmp = results.getDouble("SalaryPerDay");
                results.updateDouble("SalaryPerDay", tmp + (tmp * (percentage / 100)));
                results.updateRow();
            }
            connection.commit();
        } catch (SQLException e){
            e.printStackTrace();
        }
        Disconnector();
    }


    public void updateAllProjectsBudget(double percentage) {
        if(this.connection==null){
            Connector();
        }
        try{
            Statement stmt = connection.createStatement(
                    ResultSet.TYPE_SCROLL_SENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);

            stmt.executeQuery("SELECT Project.* FROM Project");
            ResultSet results = stmt.getResultSet();
            while (results.next())
            {
                int tmp = results.getInt("Budget");
                results.updateDouble("Budget", tmp + (tmp * (percentage / 100)));
                results.updateRow();
            }
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        Disconnector();
    }


    private double getEmployeeTotalSalary() {
        int ans = 0;
        if(this.connection==null){
            Connector();
        }
        try{
            Statement stmt = connection.createStatement();
            stmt.executeQuery("SELECT ConstructorEmployee.* FROM ConstructorEmployee");
            ResultSet results = stmt.getResultSet();
            while (results.next())
            {
                ans += results.getInt("SalaryPerDay");
            }
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        Disconnector();
        return (double)ans;
    }


    private int getTotalProjectBudget() {
        int ans = 0;
        if(this.connection==null){
            Connector();
        }
        try{
            Statement stmt = connection.createStatement();
            stmt.executeQuery("SELECT Project.* FROM Project");
            ResultSet results = stmt.getResultSet();
            while (results.next())
            {
                ans += results.getInt("Budget");
            }
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        Disconnector();
        return ans;
    }

    private void dropDB() {
        if(this.connection==null){
            Connector();
        }
        String sql = "DROP DATABASE DB2019_Ass2";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.executeUpdate();
            connection.commit();
            Disconnector();
        } catch (SQLException e) {
            e.printStackTrace();
        }catch(Exception e2){
            e2.printStackTrace();
        }
    }


    private void initDB(String csvPath) {
        if(this.connection==null){
            Connector();
        }
        try {
            String line;
            Process p = Runtime.getRuntime().exec("cmd.exe /c psql -U this.username -d DB2019_Ass2 -h localhost -f csvPath");
            BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            while ((line = input.readLine()) != null) {
                System.out.println(line);
            }
            input.close();
            Disconnector();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    private int calculateIncomeFromParking(int year) {
        YearMonth YMStart = YearMonth.of(year,1);
        YearMonth YMFinish = YearMonth.of(year,12);
        LocalDate StartDate = YMStart.atDay(1);
        LocalDate FinishDate = YMFinish.atEndOfMonth();
        int ans = 0;
        if(this.connection==null){
            Connector();
        }
        String sql = "SELECT CarParking.* FROM CarParking WHERE StartTime >= ? AND EndTime  <= ?";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setDate(1, Date.valueOf(StartDate));
            pstmt.setDate(2, Date.valueOf(FinishDate));
            ResultSet rs = pstmt.executeQuery();
            while (rs.next())
            {
                ans += rs.getInt("Cost");
            }
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        Disconnector();
        return ans;
    }

    private ArrayList<Pair<Integer, Integer>> getMostProfitableParkingAreas() {
        if(this.connection==null){
            Connector();
        }
        ArrayList<Pair<Integer, Integer>> ProfitParkingAreas = new ArrayList<>();int counter = 0;
        try{
            Statement stmt = connection.createStatement();
            stmt.executeQuery("SELECT AID,maxpriceperday FROM ParkingArea ORDER BY  maxpriceperday DESC");
            ResultSet results = stmt.getResultSet();
            while (results.next() && counter < 5)
            {
                int aid = results.getInt("AID");
                int max = results.getInt("maxpriceperday");
                ProfitParkingAreas.add(new Pair<>(aid,max));
                counter++;
            }
            connection.close();
        } catch (SQLException e){
            e.printStackTrace();
        }
        Disconnector();
        return ProfitParkingAreas;
    }

    private ArrayList<Pair<Integer, Integer>> getNumberOfParkingByArea() {
        if(this.connection==null){
            Connector();
        }
        ArrayList<Pair<Integer, Integer>> NumParkingAreas = new ArrayList<>(); //)");
        try{
            Statement stmt = connection.createStatement();
            stmt.executeQuery("SELECT ParkingAreaID,COUNT (*) AS ParkingCounter FROM CarParking GROUP BY ParkingAreaID");
            ResultSet results = stmt.getResultSet();
            while (results.next())
            {
                int aid = results.getInt("ParkingAreaID");
                int num = results.getInt("ParkingCounter");
                NumParkingAreas.add(new Pair<>(aid,num));
            }
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        Disconnector();
        return NumParkingAreas;
    }


    private ArrayList<Pair<Integer, Integer>> getNumberOfDistinctCarsByArea() {
        if(this.connection==null){
            Connector();
        }
        ArrayList<Pair<Integer, Integer>> NumDistinctCarsByArea = new ArrayList<>();
        try{
            Statement stmt = connection.createStatement();
            stmt.executeQuery("SELECT ParkingAreaID,COUNT(DISTINCT CID) AS CarCounter FROM CarParking GROUP BY ParkingAreaID");
            ResultSet results = stmt.getResultSet();
            while (results.next())
            {
                int aid = results.getInt("ParkingAreaID");
                int num = results.getInt("CarCounter");
                NumDistinctCarsByArea.add(new Pair<>(aid,num));
            }
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        Disconnector();
        return NumDistinctCarsByArea;
    }


    private void AddEmployee(int EID, String LastName, String FirstName, Date BirthDate, String StreetName, int Number, int door, String City) {
        if(this.connection==null){
            Connector();
        }
        String sql = "INSERT INTO Employee(EID,LastName,FirstName,BirthDate,StreetName,Number,door,City) VALUES(?,?,?,?,?,?,?,?)";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setInt(1, EID);
            pstmt.setString(2, LastName);
            pstmt.setString(3, FirstName);
            pstmt.setDate(4, BirthDate);
            pstmt.setString(5, StreetName);
            pstmt.setInt(6, Number);
            pstmt.setInt(7, door);
            pstmt.setString(8, City);
            pstmt.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        Disconnector();
    }
}


//    String testFuncStart1 = "initDB,/Users/eranedri/Documents/test/data/DB2019_Project_Ass4_DDL.sql";
//    String testFuncStart2 = "initDB,/Users/eranedri/Documents/test/data/gen.sql";
//    String testFuncFinish = "dropDB";
//    String[] test1 = testFuncStart1.split(cvsSplitBy);
//    String[] test2 = testFuncStart2.split(cvsSplitBy);
//    String[] test3 = testFuncFinish.split(cvsSplitBy);
//    executeFunc(ass, test1);
//    executeFunc(ass, test2);
//    executeFunc(ass, test3);
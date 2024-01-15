package org.example;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.lang.reflect.*;
import java.time.LocalDateTime;
import java.io.*;
import java.util.*;
import java.util.zip.*;

//add BrestAirport MoscowAirpot +99999999-12-31T23:59:59.99999999 +99999999-12-31T23:59:59.99999999 1096 UNAVAILABLE routes

//add Misha Belkin PILOT SICK workers
public class Main {

    public static final Logger logger = Logger.getLogger(Main.class);

    static {
        PropertyConfigurator.configure("src/main/resources/log4j.properties");
    }

    static Table<Worker> workerTable;
    static Table<Route> routeTable;
    static Table<Plane> planeTable;


    public static void main(String[] args) {
        Main.logger.info("Hello world!");

        try(ZipInputStream zin = new ZipInputStream(new FileInputStream("./src/main/resources/database.zip")))
        {
            ZipEntry entry;
            String name;
            while((entry=zin.getNextEntry())!=null){

                name = "./src/main/resources/"+entry.getName(); // получим название файла
                System.out.printf("File name: %s \n", name);

                // распаковка
                FileOutputStream fout = new FileOutputStream( name);
                for (int c = zin.read(); c != -1; c = zin.read()) {
                    fout.write(c);
                }
                fout.flush();
                zin.closeEntry();
                fout.close();
            }
        }
        catch(Exception ex){

            System.out.println(ex.getMessage());
        }

        workerTable = new Table<>(Worker.getFileName(), Worker.getColNames(), Worker.class);
        routeTable =  new Table<>(Route.getFileName(), Route.getColNames(), Route.class);
        planeTable = new Table<>(Plane.getFileName(), Plane.getColNames(), Plane.class);

        String inputString = new String();

        Main.logger.info("worker SELECT");

        workerTable.select(new String[]{"name", "serName"}).printTable();
        Main.logger.info("worker SELECT end");
        Main.logger.info(String.join(", ", workerTable.getArrayRowFromList(0)));

        ArrayList<Table> tables = new ArrayList<>(2);
        tables.add(routeTable);
        tables.add(planeTable);
        String[] joinCols = new String[] {"planeId", "id"};

        Table<BaseRow> fdfdg = routeTable.join(planeTable, joinCols);

        Main.logger.info("TRY sort");
        workerTable.sort("name", SortOrder.REVERSEALPHABET).select(new String[]{
                "name", "serName", "profession", "status", "routeId"}).find("routeId", "1").printTable();

        User user = new User();

        //user.startWork();

        Admin admin = new Admin();

        admin.startWork();

        workerTable.writeFile();
        routeTable.writeFile();
        planeTable.writeFile();

        try(ZipOutputStream zout = new ZipOutputStream(new FileOutputStream("./src/main/resources/database.zip"));
            ) {
            String[] fileNames = new String[]{
                    workerTable.getFileName(),
                    routeTable.getFileName(),
                    planeTable.getFileName()};

            for (String fileName : fileNames) {
                FileInputStream fis = new FileInputStream("./src/main/resources/"+fileName);
                ZipEntry entry = new ZipEntry(fileName);
                zout.putNextEntry(entry);
                // считываем содержимое файла в массив byte
                byte[] buffer = new byte[fis.available()];
                fis.read(buffer);
                // добавляем содержимое к архиву
                zout.write(buffer);
                // закрываем текущую запись для новой записи
                zout.closeEntry();
            }
        }
        catch(Exception ex){

            System.out.println(ex.getMessage());
        }


    }





    static public void printResult(){


        String report = new String("");

        Table<BaseRow> routePlanes = routeTable.join(planeTable, new String[]{"planeId", "id"});

        if (routePlanes.getSize()== routeTable.getSize()) {

            for (int i = 0; i < routeTable.getSize(); i++) {
                String[] row = routePlanes.getArrayRowFromList(i);

                report += String.join(", ", routePlanes.getCols());
                report += "\n";
                report += String.join(", ", row);

                report += "\nWorkers: \n";
                report += workerTable.find("routeId", row[0]).tableToString();

                report +="\n\n\n";
            }
            System.out.println("\n\n\n");
            System.out.println(report);

            try (BufferedWriter bufferedWriter = new BufferedWriter( new FileWriter("report.csv"))){

                bufferedWriter.write(report);

                bufferedWriter.close();
            }
            catch (IOException ex){
                Main.logger.error(ex.getMessage());
            }

        } else {
            logger.error("no such plane");
        }


    }
}

class User {

    protected Map<String, Method> methodMap = new HashMap<>();

    static protected final String[] commands = {"select", "sort", "find", "join"};
    User(){
        try {
            this.methodMap.put("select", Table.class.getDeclaredMethod("select", String[].class));
            this.methodMap.put("sort", Table.class.getDeclaredMethod("sort", String.class, SortOrder.class));
            this.methodMap.put("find", Table.class.getDeclaredMethod("find", String.class, String.class));
            this.methodMap.put("join", Table.class.getDeclaredMethod("join", Table.class, String[].class));

        } catch (Exception e){
            Main.logger.error(e.getMessage());
        }
    }


    public void startWork() {

        Scanner scanner = new Scanner(System.in);


        while (true) {
            final String[] inputStr = scanner.nextLine().split(" +");
            if (inputStr[0].equalsIgnoreCase("exit")) break;
            else if (inputStr[0].equalsIgnoreCase("report"))
            {
                Main.printResult();
                break;
            }

            Stack<String> commandStack = new Stack<>();
            Stack<ArrayList<String>> paramsStack = new Stack<>();

            String tableName = inputStr[inputStr.length - 1];
            Table table = Table.getTableByName(tableName);

            if (table != null) {
                try {
                    ArrayList<String> paramStr = new ArrayList<>();
                    for (int i = 0; i < inputStr.length - 1; i++) {

                        final String tempStr = inputStr[i];
                        if (Arrays.stream(commands).anyMatch(el -> el.equalsIgnoreCase(tempStr))) {
                            commandStack.add(inputStr[i]);
                            if (i!=0) {
                                paramsStack.add(new ArrayList<>(paramStr));
                            }
                            paramStr.clear();

                        } else  if (inputStr[i].equalsIgnoreCase("from")){
                            continue;
                        } else {
                            paramStr.add(inputStr[i]);
                        }

                        if (i>=inputStr.length-1){
                            Main.logger.error("Bad arguments or missed Table");
                            break;
                        }
                    }
                    paramsStack.add(new ArrayList<>(paramStr));

                } catch (ArrayIndexOutOfBoundsException e) {
                    Main.logger.error(e.getMessage());
                    Main.logger.info("Wrong combination of commands and arguments");

                } catch (Exception e) {
                    Main.logger.error(e.getMessage());

                }
            } else {
                Main.logger.info("Error: no such table");

            }

            processor(table, commandStack, paramsStack);
        }

        scanner.close();

    }

    protected void processor(Table table, Stack<String> commandStack, Stack<ArrayList<String>> paramsStack){

        while (!commandStack.isEmpty()){
            String command = commandStack.pop();
            ArrayList<String> props = paramsStack.pop();

            try {


                if (command.equalsIgnoreCase(commands[0])) {
                    String[] propArr = new String[props.size()];
                    for (int i=0; i<propArr.length;i++){
                        propArr[i] = props.get(i);
                    }

                    table = (Table) this.methodMap.get(command).invoke(table, (Object) propArr.clone());

                } else if (command.equalsIgnoreCase(commands[1])) {

                    table = (Table) this.methodMap.get(command).invoke(table, props.get(0), props.get(1).equalsIgnoreCase("Alphabet")? SortOrder.ALPHABET : SortOrder.REVERSEALPHABET);

                } else if (command.equalsIgnoreCase(commands[2])) {

                    table = (Table) this.methodMap.get(command).invoke(table, props.get(0), props.get(1));

                } else if (command.equalsIgnoreCase(commands[3])) {

                    table = (Table) this.methodMap.get(command).invoke(table, Table.getTableByName(props.get(0)) , new String[]{props.get(1), props.get(2)});
                }

            } catch (IllegalAccessException | InvocationTargetException e) {
                Main.logger.error(e.getMessage());
                break;
            }
            
        }
        System.out.println("asd: "+table.tableToString());

    }

    // select name from workers
    // select routesid from join planes planeId id routes
    // find name Senya workers
    // sort name Alphabet workers
    // add Ann Baen STEWARDESS FINE workers

}


//add 3 Miha vert driver 2 bad workers
class Admin extends User{

    static protected final String[] commands = {"select", "sort",
            "find", "join", "add", "update", "delete", "updateStatus",
    "changeArrivingTime"};

    Admin(){
        super();
        try {
            this.methodMap.put("add", Table.class.getDeclaredMethod("add", BaseRow.class));
            this.methodMap.put("update", Table.class.getDeclaredMethod("update", long.class , BaseRow.class));
            this.methodMap.put("delete", Table.class.getDeclaredMethod("delete", long.class));

        } catch ( NoSuchMethodException e) {
            Main.logger.error(e.getMessage());
        }
    }

    @Override
    public void startWork() {

        Scanner scanner = new Scanner(System.in);


        while (true) {
            final String[] inputStr = scanner.nextLine().split(" +");
            if (inputStr[0].equalsIgnoreCase("exit")) break;
            else if (inputStr[0].equalsIgnoreCase("report")) {
                Main.printResult();
                break;
            }

            Stack<String> commandStack = new Stack<>();
            Stack<ArrayList<String>> paramsStack = new Stack<>();

            String tableName = inputStr[inputStr.length - 1];
            Table table = Table.getTableByName(tableName);

            if (table != null) {
                try {
                    ArrayList<String> paramStr = new ArrayList<>();
                    for (int i = 0; i < inputStr.length - 1; i++) {

                        final String tempStr = inputStr[i];
                        if (Arrays.stream(commands).anyMatch(el -> el.equalsIgnoreCase(tempStr))) {
                            System.err.println("command: "+inputStr[i] + " paramStr:" +String.join(", ", paramStr));
                            commandStack.add(inputStr[i]);
                            if (i!=0) {
                                paramsStack.add(new ArrayList<>(paramStr));
                            }
                            paramStr.clear();

                        } else  if (inputStr[i].equalsIgnoreCase("from")){
                            continue;
                        } else {
                            paramStr.add(inputStr[i]);
                            System.err.println("prop : "+inputStr[i]);
                        }

                        if (i>=inputStr.length-1){
                            Main.logger.error("Bad arguments or missed Table");
                            break;
                        }
                    }
                    paramsStack.add(new ArrayList<>(paramStr));
                } catch (ArrayIndexOutOfBoundsException e) {
                    Main.logger.error(e.getMessage());
                    Main.logger.info("Wrong combination of commands and arguments");

                } catch (Exception e) {
                    Main.logger.error(e.getMessage());

                }
                processor(table, commandStack, paramsStack);
            } else {
                Main.logger.info("Error: no such table");

            }


        }
    }

    @Override
    protected void processor(Table table, Stack<String> commandStack, Stack<ArrayList<String>> paramsStack){

        if (table == null){
            Main.logger.info("Objects not found");
            return;
        }
        //paramsStack.stream().forEach(System.err::println);
        while (!commandStack.isEmpty()){




            try {
                String command = commandStack.pop();
                ArrayList<String> props = paramsStack.pop();
                //System.err.println(" paramStr:" +String.join(", ", props));

                if (command.equalsIgnoreCase(commands[0])) {//select
                    String[] propArr = new String[props.size()];

                    for (int i=0; i<propArr.length;i++){
                        propArr[i] = props.get(i);
                    }
                    //System.err.println(paramsStack.pop().get(0));
                    table = (Table) this.methodMap.get(command).invoke(table, (Object) propArr.clone());

                } else if (command.equalsIgnoreCase(commands[1])) {//sort

                    table = (Table) this.methodMap.get(command).invoke(table, props.get(0), props.get(1).equalsIgnoreCase("Alphabet")? SortOrder.ALPHABET : SortOrder.REVERSEALPHABET);

                } else if (command.equalsIgnoreCase(commands[2])) {//find

                    table = (Table) this.methodMap.get(command).invoke(table, props.get(0), props.get(1));

                } else if (command.equalsIgnoreCase(commands[3])) {//join

                    table = (Table) this.methodMap.get(command).invoke(table, Table.getTableByName(props.get(0)) , new String[]{props.get(1), props.get(2)});

                } else if (command.equalsIgnoreCase(commands[4])) {//add
                    String[] propArr = new String[props.size()];
                    for (int i=0; i<propArr.length;i++){
                        propArr[i] = props.get(i);
                    }

                    try {
                        Object tableElem = table.genericClass.getDeclaredConstructor().newInstance();
                        table.genericClass.getDeclaredMethod("init", String[].class)
                                .invoke(tableElem, (Object) propArr);

                        this.methodMap.get(command).invoke(table, tableElem);

                    } catch (Exception e){
                        Main.logger.error(e.getMessage());

                    } finally {
                        break;
                    }


                } else if (command.equalsIgnoreCase(commands[5])) {//update
                    Long id=0L;

                    String[] propArr = new String[props.size()-1];
                    for (int i=0; i<propArr.length;i++){
                        propArr[i] = props.get(i+1);
                    }

                    //System.out.println(String.join(" ", propArr));
//update 14 Oksana Gorova STEWARDESS FINE workers
                    try {
                        id = Long.parseLong(props.get(0));
                        BaseRow oldElement = table.getRowById( id);
                        System.out.println("oldElem: " + oldElement.printToFile()+" id:"+Long.toString(id));
                        String[] args = oldElement.getArrayRow();

                        for (int i=1; i<propArr.length+1; i++){
                            args[i] = propArr[i-1];
                        }

                        Object tableElem = table.genericClass.getDeclaredConstructor().newInstance();
                        table.genericClass.getDeclaredMethod("init", String[].class)
                                .invoke(tableElem, (Object) args);

                        System.out.println(String.join(" ",(String[]) BaseRow.class
                                .getDeclaredMethod("getArrayRow")
                                .invoke(tableElem)));



                        this.methodMap.get(command).invoke(table, id, tableElem);
                    } catch (Exception e){
                        Main.logger.error("Update error: "+Long.toString(id)+e.getMessage());

                    } catch (Error e){
                        Main.logger.error(e.getMessage());

                    } finally {
                        break;
                    }

                } else if (command.equalsIgnoreCase(commands[6])) {//delete
                    long id;

                    try {
                        id = Long.parseLong(props.get(0));
                        this.methodMap.get(command).invoke(table, id);

                    } catch (Exception e){
                        Main.logger.error("Parsing error "+ e.getMessage());
                        break;
                    } finally {
                        break;
                    }
                } else if (command.equalsIgnoreCase(commands[7])) {//updateStatus
                    long id;
                    BaseRow row;
                    String newStatus;
                    Object updatedRow;

                    try {
                        id = Long.parseLong(props.get(0));
                        newStatus = props.get(1);
                        row = table.getRowById( id);
                        String[] args = row.getArrayRow();
                        int index =(int) table.getSelectedColsIndexes(new String[]{"status"}).get(0);
                        args[index] = newStatus;

                        updatedRow = table.genericClass.getDeclaredConstructor().newInstance();
                        table.genericClass.getDeclaredMethod("init", String[].class)
                                .invoke(updatedRow, (Object) args);

                        //table.update(id,(table.genericClass.cast(updatedRow)));
                    } catch (Exception e){
                        Main.logger.error("Error: cant parse long");
                        break;
                    } finally {
                        break;
                    }


                } else if (command.equalsIgnoreCase(commands[8])) {//changeArrivingTime
                    long id;

                    try {
                        id = Long.parseLong(props.get(0));
                        this.methodMap.get(command).invoke(table, id);

                    } catch (Exception e){
                        Main.logger.error("Error: cant parse long");
                        break;
                    } finally {
                        break;
                    }
                }

            } catch (EmptyStackException e ){
                Main.logger.info("Error: invalid arguments");
            } catch (IllegalAccessException | InvocationTargetException e) {
                Main.logger.error(e.getMessage());
            }

        }
        if (table!=null){
            table.printTable();
        } else {
            Main.logger.info("Elems not found");
            System.out.println("Elems not found");
        }
    }


}
enum SortOrder{
    ALPHABET,
    REVERSEALPHABET
}

enum WorkerStatus{
    FINE,
    SICK,
    HOLIDAY
}

enum WorkerSProfession{
    PILOT,
    STEWARDESS
}

enum RouteStatus{
    AVAILABLE,
    UNAVAILABLE
}

enum PlaneStatus{
    AVAILABLE,
    NEEDREPAIR,
    ONMAINTENANCE
}

interface Cols {
    long getId();
    String[] getArrayRow();
}
class BaseRow implements Cols {
    String[] row;

    public String[] getArrayRow() {

        Main.logger.info("base getRow");
        return this.row;
    }

    public long getId() { //optional

        return 0;
    }
    @Override
    public String toString(){
        return String.join(" ", row);
    }

    public BaseRow init(String[] args) {
        this.row = args.clone();
        return this;
    }

    public String printToFile() {
        return String.join(", ",this.row);
    }
}

class Table<T extends BaseRow>{
    public class JoinPair{
        public Table[] tables = new Table[2];
        public String[] cols= new String[2];
}

    final Class<T> genericClass;
    public final String tableName;
    private ArrayList<T> list = new ArrayList<>();
    private String fileName = new String();
    private String[] cols;

    public int getSize(){
        return this.list.size();
    }
    public String getFileName(){
        return fileName;
    }
    public String[] getCols(){
        return this.cols;
    }
    public String[] getArrayRowFromList(int index){
        return this.list.get(index).getArrayRow();
    }
    public T getRowByIndex(int index) {
        return this.list.get(index);
    }
    public T getRowById(Long index) {
        return this.list.stream().filter(elem -> elem.getId()==index).findFirst().get();
    }


    Table(String fileName, String[] cols, Class clazz){
        this.genericClass = clazz;
        this.fileName = fileName;
        this.tableName = fileName.substring(0, fileName.length()-4);
        this.cols = cols;
        Main.logger.info("tableNAme: "+this.tableName);
        Table.tableSet.add(this);

        try {
            this.readFile();
            Main.logger.info("read complete");

        } catch (Exception e){
            Main.logger.info("read error" + e.getMessage().toString());
        }

    }
    Table( ArrayList<Table> tables, String[] cols){
        this.genericClass = (Class) BaseRow.class;
        String name = new String();
        ArrayList<String> tCols= new ArrayList<String>();
        for (Table table : tables){
            name += table.tableName + " ";
            for (String col : table.cols){
                tCols.add(table.tableName + col);
            }
        }
        this.tableName = name;
        this.cols = (String[]) tCols.toArray(new String[tCols.size()]);
        Main.logger.info(String.join(", ",this.cols));

        int tableIndex0 =  (Integer) tables.get(0).getSelectedColsIndexes(new String[]{cols[0]}).get(0);
        int tableIndex1 = (Integer) tables.get(1).getSelectedColsIndexes(new String[]{cols[1]}).get(0);

        ArrayList<T> list = new ArrayList<>();

        try {
            for ( int i=0; i<tables.get(0).list.toArray().length ; i++){
                String[] row0 = tables.get(0).getArrayRowFromList(i);
                for ( int k=0; k<tables.get(1).list.toArray().length ; k++){
                    String[] row1 = tables.get(1).getArrayRowFromList(k);
                    ArrayList<String> rowList = new ArrayList<>();
                    if ( row0[tableIndex0].equals(row1[tableIndex1])) {
                        Collections.addAll(rowList, row0);
                        Collections.addAll(rowList, row1);
                        BaseRow row = new BaseRow();
                        row.init( rowList.toArray(new String[rowList.size()]));
                        this.list.add((T) row );
                    }
                }
            }
        } catch (Exception e){
            Main.logger.error("Table (tables[]) constructor err: "+e.getMessage());
        }

        //this.select(this.getCols()).printTable();

    }
    Table(ArrayList<T> arr, String[] cols, Class clazz){
        this.list = arr;
        this.cols = cols.clone();
        this.genericClass = clazz;
        this.tableName = "tempTable";
    }

//Admin methods /*
    public void add(T el){
        System.err.println(el.row[0]);
        try {
            el.row[0]=Long.toString(Long.parseLong(this.list.get(this.list.size()-1).getArrayRow()[0])+1L);

        } catch (Exception e){
            Main.logger.error("Error: cant parse long");
        }

        if (Table.tableSet.contains(this)) {
            list.add(el);
        } else {
            throw new Error("Error: Try add row to temp table");
        }
    }
    public void update(long id, T newEl){


        if (Table.tableSet.contains(this)) {
            for (T el : this.list){
                if (el.getId() == id){
                    System.out.println("AAAA: "+el.printToFile());
                    el = newEl;
                    return;
                }
            }
            Main.logger.info("row not found");

        } else {
            throw new Error("Error: Try update row in temp table");
        }
    }
    public void delete(long id){

        if (Table.tableSet.contains(this)) {
            if (this.list.removeIf(el -> {
                if(el.getId() == id) {
                    try {

                        if (this.genericClass == Worker.class) {
                            System.err.println("woreker delete start" +
                                    "\n "+ String.join(" ", el.getArrayRow())
                            +"\n "+el.getArrayRow()[5]);
                            if (!el.getArrayRow()[5].equalsIgnoreCase("null")) {
                                Worker.deleteWorkerUpdateDependencies(
                                        Long.parseLong(el.getArrayRow()[5]),
                                        Worker.validateProfession(el.getArrayRow()[3]));
                            }
                            System.err.println("woreker delete end");
                        } else if (this.genericClass == Route.class) {
                            Worker.deleteRouteIds(Long.parseLong(el.getArrayRow()[0]));
                        } else if (this.genericClass == Plane.class) {
                            Route.deletePlaneIds(Long.parseLong(el.getArrayRow()[0]));
                        }
                    } catch (NumberFormatException e){
                        Main.logger.info("no dependencies to restore");
                        return true;
                    }
                    return true;
                } else return false;

            })){
                Main.logger.info("row removed");
            } else {
                Main.logger.info("row not found");
            }

        } else {
            throw new Error("Error: Try delete row in temp table");
        }
    }
    //  */ Admin methods end
    public ArrayList<Integer> getSelectedColsIndexes(String[] selectCols){
        ArrayList<Integer> indexArr = new ArrayList<Integer>();
        int k=0;
        for (String col : selectCols){
            k=0;
            for (String tempCol : this.cols) {
                if (tempCol.equals(col)) {
                    indexArr.add(k);
                }
                k++;
            }
        }
        //Main.logger.info( indexArr.toArray() );
        return  indexArr;
    }

    //User methods /*
    public Table<BaseRow> select(String[] selectCols){

        ArrayList<BaseRow> selectedArray = new ArrayList<>();

        if (selectCols.length==1 && selectCols[0].equals("*")){
            for (T elem : this.list){
                selectedArray.add((new BaseRow().init(elem.getArrayRow().clone())));
            }

            return new Table<BaseRow>(selectedArray, this.getCols(), BaseRow.class);
        }

        ArrayList<Integer> indexes = this.getSelectedColsIndexes(selectCols);

        for (T elem : this.list){
            String[] listRow = elem.getArrayRow();
            String[] row = new String[indexes.size()];
            int i = 0;
            for ( int index : indexes){
                row[i++] = listRow[index];
            }

            selectedArray.add((new BaseRow().init(row)));
        }
        //new Table<BaseRow>(selectedArray, selectCols, BaseRow.class).printTable();
        return new Table<BaseRow>(selectedArray, selectCols, BaseRow.class);
    }

    public Table<BaseRow> sort(String col, SortOrder SORTORDER){

        ArrayList<BaseRow> sortedArray = new ArrayList<>(this.list);
        int index = getSelectedColsIndexes(new String[]{col}).get(0);
        if (SORTORDER == SortOrder.ALPHABET)    {
            sortedArray.sort(((c1, c2) -> (c1.getArrayRow()[index].compareTo(c2.getArrayRow()[index]))));
        } else if (SORTORDER == SortOrder.REVERSEALPHABET) {
            sortedArray.sort(((c1, c2) -> (c2.getArrayRow()[index].compareTo(c1.getArrayRow()[index]))));
        }

        return new Table<BaseRow>(sortedArray, this.getCols(), BaseRow.class);
    }
    public Table<BaseRow> find(String col, String compareString){

        ArrayList<BaseRow> findArray = new ArrayList<>(this.list);
        int index = getSelectedColsIndexes(new String[]{col}).get(0);

        findArray.removeIf(el -> !el.getArrayRow()[index].equals(compareString));

        return new Table<BaseRow>(findArray, this.getCols(), BaseRow.class);
    }
    public Table<BaseRow> join(Table table, String[] cols){

        if (Table.tableSet.contains(this) && Table.tableSet.contains(table)) {
            ArrayList<Table> tableArrayList = new ArrayList<Table>();
            tableArrayList.add(this);
            tableArrayList.add(table);

            return new Table<BaseRow>(tableArrayList, cols.clone());
        } else {
            throw new Error("Error: Try join temp tables");
        }
    }
    //  */ User methods end

    protected ArrayList<Long> parseLongArray(){//in: table contain 1 col with Ids, ex: table.select(new String[]{"id"}).parseIds()
        ArrayList<Long> idArr = new ArrayList<>();

        for (T elem : this.list){
            String number = elem.toString();

            if (!number.equalsIgnoreCase("null")) {
                System.err.println("number: "+number);
                idArr.add(Long.parseLong(number));
            }

        }
        System.err.println();
        return idArr;
    }

    protected static HashSet<Table> tableSet = new HashSet<Table>();
    public static Table getTableByName(String tableName){
        for (Table table : tableSet){
            if (table.tableName.equals(tableName)){
                return table;
            }
        }
        return null;
    }

    private boolean readFile () throws Exception{
        try (BufferedReader bufferdReader = new BufferedReader( new FileReader("./src/main/resources/"+this.fileName))){
            String buf = bufferdReader.readLine();//skip first string

            Main.logger.info("first line is: "+buf);

            while ((buf = bufferdReader.readLine()) != null){
                Main.logger.info("second line is: "+buf);
                Main.logger.info("buf split: "+buf.split("[,\\s]+")[3]);
                T elem = this.genericClass.getDeclaredConstructor().newInstance();
                Main.logger.info("add el" );
                elem.init(buf.split("[,\\s]+"));
                this.list.add( (T) elem );
            }
            bufferdReader.close();
            return true;
        }
        catch (IOException ex){
            Main.logger.info(ex.getMessage());
        }

        return false;
    }
    public boolean writeFile () {

        try (BufferedWriter bufferedWriter = new BufferedWriter( new FileWriter("./src/main/resources/"+this.fileName))){
            String buf = new String();//skip first string

            buf = String.join(", ", this.getCols()) + "\n";
            buf += this.tableToString();

            Main.logger.info("file is: "+buf);
            bufferedWriter.write(buf);

            bufferedWriter.close();
            return true;
        }
        catch (IOException ex){
            Main.logger.error(ex.getMessage());
        }

        return false;
    }

    public void printTable(){
        System.out.println("\n\n\n");
        System.out.println(String.join(" ", getCols()));

        this.list.stream().forEach(el ->System.out.println(el.printToFile()));
    }

    public String tableToString(){
        String report = new String();
        for (BaseRow row : this.list){
            report += String.join(", ", row.getArrayRow())+"\n";
        }

        return report;
    }
}

class Worker extends BaseRow{

    private Long id;
    private String name;
    private String serName;
    private WorkerSProfession profession;
    private Long routeId;
    private WorkerStatus status;//
    private static long nextId = 0;//get last id from table
    private final static String[] colNames = {"id", "name", "serName", "profession", "status", "routeId"};

    public static String getFileName(){
        return "workers.txt";
    }

    public Worker init(String name, String serName, WorkerSProfession profession, WorkerStatus status, Long routeId){
        this.id = nextId++;
        this.name = name;
        this.serName = serName;
        this.profession = profession;
        this.status = status;
        this.routeId = routeId;
        return this;
    }

    public Worker init(String[] args){
        try {
            if (args.length == 6) {
                this.id = Long.parseLong(args[0]);
                this.name = args[1];
                this.serName = args[2];
                this.profession = validateProfession(args[3]);
                this.status = validateStatus(args[4]);
                if (!args[5].equalsIgnoreCase("null")) {
                    this.routeId = Long.parseLong(args[5]);
                } else {
                    this.routeId=null;
                }
                if (nextId<=this.id) nextId = this.id+1;

                this.row = args;
                return this;

            } else if (args.length == 4) {
                this.id = nextId++;
                this.name = args[0];
                this.serName = args[1];
                this.profession = validateProfession(args[2]);
                this.status = validateStatus(args[3]);

                this.row = new String[]{Long.toString(this.id),
                        args[0], args[1], args[2], args[3],
                        "null"};

                try {
                    if (this.status == WorkerStatus.FINE) {
                        this.routeId = getRouteId();
                        if (this.routeId != null) {
                            this.row[5] = Long.toString(this.routeId);
                        }
                    } else {
                        this.routeId = null;
                    }
                } catch (Error |NumberFormatException e) {
                    if (e.getMessage().equals("Not find free route")) {
                        Main.logger.info(e.getMessage());
                    }
                }
                return this;
            } else {
                throw new Error("Parsing error: not enough arguments");
            }




        } catch (NumberFormatException e) {
            Main.logger.error("parsing error:" + e.getMessage());
            throw new Error("parsing error:" + e.getMessage());
        }

    }

    private class RouteBrigade{
        long routeId;
        Long pilotId;
        ArrayList<Long> workersIds= new ArrayList<>();

        RouteBrigade(long routeId, Table table){
            this.routeId = routeId;

            table = table.find("routeId", Long.toString(routeId))
                    .find("status", "FINE");


            Table tableTemp;

            tableTemp = table.find(colNames[3], "PILOT");
            if (tableTemp.getSize() != 0){
                this.pilotId = Long.parseLong(
                        tableTemp.select(new String[]{"id"}).getArrayRowFromList(0)[0]);
            } else {
                Main.logger.info("no Pilots");
            }

            tableTemp = table.find(colNames[3], "STEWARDESS");
            if (tableTemp.getSize() != 0) {
                this.workersIds = tableTemp.select(new String[]{"id"}).parseLongArray();
                System.err.println("worker ids: "+workersIds);
            }

        }

        public boolean isFull(){
            if (pilotId != null && workersIds.size()>=5){
                return true;
            } else {
                return false;
            }
        }

    }

    protected Long getRouteId(){

        ArrayList<Long> idArrRoute = Main.routeTable.select(new String[]{"id"}).parseLongArray();
        ArrayList<RouteBrigade> routeBrigades = new ArrayList<>();
        System.err.println("idArrRoute: "+idArrRoute);
        for (long id : idArrRoute){
            routeBrigades.add(new RouteBrigade(id, Main.workerTable));
        }
        routeBrigades.removeIf(elem -> elem.isFull());
        if (this.profession == WorkerSProfession.PILOT){
            for (RouteBrigade brigade : routeBrigades){
                if (brigade.pilotId == null){
                    return brigade.routeId;
                }
            }
        } else {
            for (RouteBrigade brigade : routeBrigades){
                if (brigade.workersIds.size()<5){
                    return brigade.routeId;
                }
            }
        }
        return null;
    }

    public static void setWorkersForRoute(Long routeId){

        ArrayList<Long> freePilots = Main.workerTable.find("status", "FINE")
                .find("routeId", "null")
                .find("profession", "PILOT")
                .select(new String[]{"id"}).parseLongArray();

        ArrayList<Long> freeStewardess = Main.workerTable.find("status", "FINE")
                .find("routeId", "null")
                .find("profession", "STEWARDESS")
                .select(new String[]{"id"}).parseLongArray();

        if (!freePilots.isEmpty()){
            Long id = freePilots.get(0);

            Worker worker = Main.workerTable.getRowById(id);
            worker.row[5] = Long.toString(routeId);
            worker.routeId = routeId;

        }

        int StewardessCount = 0;
        for (Long id : freeStewardess){
            ++StewardessCount;
            Worker worker = Main.workerTable.getRowById(id);
            worker.row[5] = Long.toString(routeId);
            worker.routeId = routeId;
            if (StewardessCount == 5) break;

        }
    }
    public static WorkerSProfession validateProfession(String profession){
        if (profession.equalsIgnoreCase("PILOT")){
            return WorkerSProfession.PILOT;
        } else if (profession.equalsIgnoreCase("STEWARDESS")){
            return WorkerSProfession.STEWARDESS;
        } else {
            System.err.println("validateProfession err");
            throw new Error("Profession parsing error");
        }
    }

    protected WorkerStatus validateStatus(String status){
        if (status.equalsIgnoreCase("FINE")){
            return WorkerStatus.FINE;
        } else if (status.equalsIgnoreCase("SICK")){
            return WorkerStatus.SICK;
        } else if (status.equalsIgnoreCase("HOLIDAY")) {
            return WorkerStatus.HOLIDAY;
        } else {
            System.err.println("validateStatus err");
            throw new Error("Status parsing error");
        }
    }

    static public void deleteWorkerUpdateDependencies(Long routeId, WorkerSProfession profession){
        Main.logger.info("Delete worker, set new worker");
        if (routeId==null) return;

        String professionStr = profession==WorkerSProfession.PILOT? "PILOT":"STEWARDESS";



        Long freeWorkerId = Long.parseLong(Main.workerTable
                .find("status", "FINE").find("profession", professionStr)
                .find("routeId", "null")
                .getRowByIndex(0).getArrayRow()[0]);



        Worker worker= (Worker) Main.workerTable.getRowById(freeWorkerId);

        if (worker!=null){
            worker.routeId=routeId;
            worker.row[5] = Long.toString(routeId);
            Main.logger.info("Deleted worker replaced");
            return;
        }
        Main.logger.info("Deleted worker not replaced");
    }

    public static void deleteRouteIds(Long routeId){
        Main.logger.info("Delete route ids in worker table");
        ArrayList<Long> workersIdsToClearRoutes = new ArrayList<>();

        Table tempTable = Main.workerTable
                .find("routeId", Long.toString(routeId))
                .select(new String[]{"id"});

        tempTable.printTable();

        if (tempTable==null) return;

        workersIdsToClearRoutes = tempTable.parseLongArray();

        workersIdsToClearRoutes.forEach(
                id -> {
                    Main.workerTable.getRowById(id).routeId=null;
                    Main.workerTable.getRowById(id).row[5]="null";
                }
        );
    }
    public static void initLastId(long lastId){
        nextId = ++lastId;
    }

    @Override
    public String[] getArrayRow(){
        Main.logger.info("worker getArrayRow");
        //Main.logger.error("id = " +Long.toString(this.id));
        return this.row;// new String[]{ Long.toString(id),name,serName,profession,Long.toString(routeId),status};
    }
    public static String[] getColNames(){
        return colNames;
    }

    public long getId(){
        System.err.println("id "+Long.toString(id)+" row="+this.printToFile());
        return this.id;
    }
}

class Route extends BaseRow{
    private Long id;
    private String airportFrom;
    private String airportIn;
    private LocalDateTime departureDateTime;
    private LocalDateTime arrivingDateTime;
    private int flyLength;
    private RouteStatus status;//active inactive
    private Long planeId;
    private static long nextId = 0;//get last id from table
    private static String[] colNames = {"id", "airportFrom", "airportIn", "departureDateTime", "arrivingDateTime", "flyLength", "status", "planeId"};
    public static String getFileName(){
        return "routes.txt";
    }

    public Route init(String airportFrom, String airportIn, LocalDateTime departureDateTime
    , LocalDateTime arrivingDateTime, int flyLength, RouteStatus status, Long planeId){
        this.id = nextId++;
        this.airportFrom = airportFrom;
        this.airportIn = airportIn;
        this.departureDateTime = departureDateTime;
        this.arrivingDateTime = arrivingDateTime;
        this.flyLength = flyLength;
        this.planeId = planeId;
        this.status = status;
        return this;
    }

    public Route init(String[] args){
        try {
            if (args.length == 8) {
                this.id = Long.parseLong(args[0]);
                this.airportFrom = args[1];
                this.airportIn = args[2];
                this.departureDateTime = LocalDateTime.parse(args[3]);
                this.arrivingDateTime = LocalDateTime.parse(args[4]);
                this.flyLength = Integer.parseInt(args[5]);
                this.status = verifyStatus(args[6]);
                if (!args[7].equalsIgnoreCase("null")) {
                    this.planeId = Long.parseLong(args[7]);
                } else {
                    this.planeId=null;
                }
                if (nextId<this.id) nextId = this.id+1;
                this.row = args.clone();
                return this;

            } else if (args.length == 6) {
                this.id = nextId++;
                this.airportFrom = args[0];
                this.airportIn = args[1];
                this.departureDateTime = LocalDateTime.parse(args[2]);
                this.arrivingDateTime = LocalDateTime.parse(args[3]);
                this.flyLength = Integer.parseInt(args[4]);
                this.status = verifyStatus(args[5]);

                this.row = new String[]{Long.toString(this.id),
                        args[0], args[1], args[2], args[3], args[4], args[5],
                        "null"};

                try {
                    System.err.println("try get plId");
                    this.planeId = getPlaneId();
                    this.row[7] = (this.planeId!=null)? Long.toString(this.planeId):"null";
                    System.err.println("getted plId =" + this.planeId);

                } catch (Error e){
                    Main.logger.error(e.getMessage());
                }
                Worker.setWorkersForRoute(this.id);

                return this;
            } else {
                throw new Error("Parsing error: not enough arguments");
            }//add BrestAirport MoscowAirpot2 +99999999-12-31T23:59:59.99999999 +99999999-12-31T23:59:59.99999999 1096 UNAVAILABLE routes

        } catch (NumberFormatException e) {
            Main.logger.error("parsing error1" + e.getMessage());
            throw new Error("parsing error" + e.getMessage());
        } catch (Exception e) {
            Main.logger.error("parsing error2" + e.getMessage());
            throw new Error("parsing error" + e.getMessage());
        }
    }

    private Long getPlaneId(){
        ArrayList<Long> idArrRoutePlanes;
        try {
            idArrRoutePlanes = Main.routeTable.select(new String[]{"planeId"}).parseLongArray();

         } catch (NumberFormatException e){
            Main.logger.info("No planes available");
            return null;
        }
        ArrayList<Long> idArrPlanes = Main.planeTable.select(new String[]{"id"}).parseLongArray();

        System.err.println(idArrRoutePlanes);
        System.err.println("\n" +idArrPlanes);

        for (long id : idArrRoutePlanes){
            idArrPlanes.remove(id);
        }
        System.err.println("\n idArrPlanes2:" +idArrPlanes);
        if (idArrPlanes.isEmpty()) {
            Main.logger.info("No planes available");
            return null;
        } else {
            return idArrPlanes.get(0);
        }
    }
    private RouteStatus verifyStatus(String status){
        if (status.equalsIgnoreCase("AVAILABLE")){
            return RouteStatus.AVAILABLE;
        } else if (status.equalsIgnoreCase("UNAVAILABLE")){
            return RouteStatus.UNAVAILABLE;
        } else {
            throw new Error("Status parsing error");
        }
    }
    public static void deletePlaneIds (Long planeId){
        Main.logger.info("Delete plane ids in route table");
        ArrayList<Long> routeIdsToClearPlanes = new ArrayList<>();

        Table tempTable = Main.routeTable
                .find("planeId", Long.toString(planeId))
                .select(new String[]{"id"});

        if (tempTable==null) return;

        tempTable.printTable();
        routeIdsToClearPlanes = tempTable.parseLongArray();

        ArrayList<Long> freePlaneIds = new ArrayList<>();
        ArrayList<Long> ocupatedPlaneIds = new ArrayList<>();

        freePlaneIds = Main.planeTable.select(new String[]{"id"})
                .parseLongArray();

        ocupatedPlaneIds = Main.routeTable.select(new String[]{"planeId"})
                        .parseLongArray();

        System.err.println("err0");

        freePlaneIds.removeIf(id -> id==planeId);

        for (Long id : ocupatedPlaneIds){
            freePlaneIds.removeIf(freeId -> freeId==id);
        }

        final Long newPlaneId;
        final String newPlaneIdStr;

        System.err.println("err1");
        if (!freePlaneIds.isEmpty()){
            newPlaneId = freePlaneIds.get(0);
            newPlaneIdStr = Long.toString(newPlaneId);
        } else {
            newPlaneId = null;
            newPlaneIdStr = "null";
        }
        System.err.println("err2");
        routeIdsToClearPlanes.forEach(
                id -> {
                    Main.routeTable.getRowById(id)
                            .planeId=newPlaneId;
                    Main.routeTable.getRowById(id)
                            .row[7]=newPlaneIdStr;
                }
        );
        System.err.println("err end");

    }
    public static void initLastId(long lastId){
        nextId = ++lastId;
    }

    public static String[] getColNames(){
        return colNames;
    }
    public long getId(){
        return this.id;
    }
}

class Plane extends BaseRow{
    private Long id;
    private String model;
    private int maxPeopleCapacity;
    private int maxFlyLength;
    private PlaneStatus status;//active inactive
    private static long nextId = 0;

    private static String[] colNames = {"id", "model", "maxPeopleCapacity", "maxFlyLength", "status"};
    public static String getFileName(){
        return "planes.txt";
    }
    public Plane init(String model, int maxPeopleCapacity, int maxFlyLength, PlaneStatus status){
        this.id = nextId++;
        this.model = model;
        this.maxPeopleCapacity = maxPeopleCapacity;
        this.maxFlyLength = maxFlyLength;
        this.status = status;
        return this;

    }
    @Override
    public Plane init(String[] args){
        try {
            if (args.length == 5) {
                this.id = Long.parseLong(args[0]);
                this.model = args[1];
                this.maxPeopleCapacity = Integer.parseInt(args[2]);
                this.maxFlyLength = Integer.parseInt(args[3]);
                this.status = verifyStatus(args[4]);

                this.row = args.clone();
                if (nextId<this.id) nextId = this.id+1;

                return this;
            } else if (args.length ==4) {
                this.id = nextId++;
                this.model = args[0];
                this.maxPeopleCapacity = Integer.parseInt(args[1]);
                this.maxFlyLength = Integer.parseInt(args[2]);
                this.status = verifyStatus(args[3]);
                return this;
            } else {
                throw new Error("parsing error");
            }

        } catch (NumberFormatException e) {
            Main.logger.error("parsing error, "+e.getMessage());
            throw new Error("parsing error");
        }
    }

    protected PlaneStatus verifyStatus(String status){
        if (status.equalsIgnoreCase("AVAILABLE")){
            return PlaneStatus.AVAILABLE;
        } else if (status.equalsIgnoreCase("NEEDREPAIR")) {
            return PlaneStatus.NEEDREPAIR;
        } else if (status.equalsIgnoreCase("ONMAINTENANCE")) {
            return PlaneStatus.ONMAINTENANCE;
        }
        throw new Error("Parsing status error");
    }
    public static void initLastId(long lastId){
        nextId = ++lastId;
    }
    public static String[] getColNames(){
        return colNames;
    }
    @Override
    public long getId(){
        return this.id;
    }
}
import java.io.IOException; 
import java.nio.file.Files; 
import java.nio.file.Path; 
import java.time.LocalDate; 
import java.time.format.DateTimeFormatter; 
import java.util.*; 
import java.util.stream.Collectors; 
import java.util.stream.Stream; 
/** 
 * A general-purpose Java program that: 
 * - Reads orders from a CSV (or uses built-in sample data if no file is given) 
 * - Parses and validates records 
 * - Computes aggregates using Java Streams 
 * - Demonstrates error handling and clean output 
 * 
 * CSV format (with header): 
 * order_id,customer,status,amount,date 
 * 1001,Alice,DELIVERED,120.50,2024-05-01 
 * 1002,Bob,CANCELLED, 75.00,2024-05-02 
 */ 
public class OrderProcessor { 
 // Accepted statuses for demonstration 
 enum Status { NEW, PROCESSING, DELIVERED, CANCELLED } 
 public static void main(String[] args) { 
 Optional<Path> csvPath = args.length > 0 ? Optional.of(Path.of(args[0])) : Optional.empty(); 
 List<Order> orders = csvPath.isPresent() 
 ? readOrdersFromCsv(csvPath.get()) 
 : sampleOrders(); 
 if (orders.isEmpty()) { 
 System.out.println("No orders to process."); 
 return; 
 } 
 // 1) Basic metrics 
 double totalRevenueDelivered = orders.stream() 
 .filter(o -> o.status == Status.DELIVERED) 
 .mapToDouble(o -> o.amount) 
 .sum(); 
 long cancelledCount = orders.stream() 
 .filter(o -> o.status == Status.CANCELLED) 
 .count(); 
 // 2) Revenue by customer (DELIVERED only) 
 Map<String, Double> revenueByCustomer = orders.stream() 
 .filter(o -> o.status == Status.DELIVERED) 
 .collect(Collectors.groupingBy( 
 o -> o.customer, 
 Collectors.summingDouble(o -> o.amount) 
 )); 
 // 3) Top N customers by revenue 
 int TOP_N = 3; 
 List<Map.Entry<String, Double>> topCustomers = revenueByCustomer.entrySet().stream() 
 .sorted(Map.Entry.<String, Double>comparingByValue().reversed()) 
 .limit(TOP_N) 
 .toList(); 
 // 4) Monthly revenue (YYYY-MM) 
 Map<String, Double> monthlyRevenue = orders.stream() 
 .filter(o -> o.status == Status.DELIVERED) 
 .collect(Collectors.groupingBy( 
 o -> o.date.getYear() + "-" + String.format("%02d", o.date.getMonthValue()), 
 Collectors.summingDouble(o -> o.amount) 
 )); 
 // 5) Simple quality checks 
 long invalidOrders = orders.stream().filter(o -> !o.isValid()).count(); 
 // ------- Output ------- 
 System.out.println("=== Order Summary ==="); 
 System.out.printf("Total orders: %d%n", orders.size()); 
 System.out.printf("Delivered revenue: %.2f%n", totalRevenueDelivered); 
 System.out.printf("Cancelled count: %d%n", cancelledCount); 
 System.out.printf("Invalid orders (flagged by parser): %d%n", invalidOrders); 
 System.out.println(); 
 System.out.println("=== Revenue by Customer (DELIVERED) ==="); 
 revenueByCustomer.entrySet().stream() 
 .sorted(Map.Entry.<String, Double>comparingByValue().reversed()) 
 .forEach(e -> System.out.printf(" %s -> %.2f%n", e.getKey(), e.getValue())); 
 System.out.println(); 
 System.out.println("=== Top Customers ==="); 
 for (int i = 0; i < topCustomers.size(); i++) { 
 var e = topCustomers.get(i); 
 System.out.printf(" %d) %s: %.2f%n", i + 1, e.getKey(), e.getValue()); 
 } 
 System.out.println(); 
 System.out.println("=== Monthly Revenue (DELIVERED) ==="); 
 monthlyRevenue.entrySet().stream() 
 .sorted(Map.Entry.comparingByKey()) 
 .forEach(e -> System.out.printf(" %s -> %.2f%n", e.getKey(), e.getValue())); 
 System.out.println(); 
 System.out.println("=== Sample Records (first 5) ==="); 
 orders.stream().limit(5).forEach(o -> System.out.println(" " + o)); 
 } 
 /** Reads orders from a CSV file. Skips header. Skips invalid lines with a warning. */ 
 private static List<Order> readOrdersFromCsv(Path path) { 
 List<Order> results = new ArrayList<>(); 
 DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd"); 
 int[] counters = new int[]{0, 0}; // [parsed, skipped] 
 try (Stream<String> lines = Files.lines(path)) { 
 Iterator<String> it = lines.iterator(); 
 boolean headerSkipped = false; 
 while (it.hasNext()) { 
 String line = it.next().trim(); 
 if (!headerSkipped) { 
 headerSkipped = true; // assume first line is header 
 continue; 
 } 
 if (line.isEmpty()) continue; 
 String[] parts = line.split(",", -1); 
 if (parts.length < 5) { 
 counters[1]++; 
 System.err.println("WARN: Skipping malformed line (too few cols): " + line); 
 continue; 
 } 
 try { 
 String orderId = parts[0].trim(); 
 String customer = parts[1].trim(); 
 Status status = Status.valueOf(parts[2].trim().toUpperCase()); 
 double amount = Double.parseDouble(parts[3].trim()); 
 LocalDate date = LocalDate.parse(parts[4].trim(), fmt); 
 Order o = new Order(orderId, customer, status, amount, date); 
 results.add(o); 
 counters[0]++; 
 } catch (Exception ex) { 
 counters[1]++; 
 System.err.println("WARN: Skipping malformed line: " + line + " (" + ex.getMessage() + ")"); 
 } 
 } 
 } catch (IOException ioe) { 
 System.err.println("ERROR: Failed to read file: " + path + " (" + ioe.getMessage() + ")"); 
 } 
 System.out.printf("Parsed %d orders, skipped %d invalid lines%n", counters[0], counters[1]); 
 return results; 
 } 
 /** Built-in sample data if no file is provided. */ 
 private static List<Order> sampleOrders() { 
 return List.of( 
 new Order("1001", "Alice", Status.DELIVERED, 120.50, LocalDate.parse("2024-05-01")), 
 new Order("1002", "Bob", Status.CANCELLED, 75.00, LocalDate.parse("2024-05-02")), 
 new Order("1003", "Alice", Status.DELIVERED, 49.99, LocalDate.parse("2024-06-05")), 
 new Order("1004", "Charlie", Status.NEW, 39.90, LocalDate.parse("2024-07-10")), 
 new Order("1005", "Bob", Status.DELIVERED, 275.00, LocalDate.parse("2024-07-12")), 
 new Order("1006", "Dana", Status.DELIVERED, 15.00, LocalDate.parse("2024-07-15")) 
 ); 
 } 
} 
/** Simple domain class with validation and readable toString(). */ 
class Order { 
 final String orderId; 
 final String customer; 
 final OrderProcessor.Status status; 
 final double amount; 
 final LocalDate date; 
 Order(String orderId, String customer, OrderProcessor.Status status, double amount, LocalDate date) { 
 this.orderId = orderId; 
 this.customer = customer; 
 this.status = status; 
 this.amount = amount; 
 this.date = date; 
 } 
 boolean isValid() { 
 return orderId != null && !orderId.isBlank() 
 && customer != null && !customer.isBlank() 
 && status != null 
 && amount >= 0.0 
 && date != null; 
 } 
 @Override 
 public String toString() { 
 return "Order{" + 
 "orderId='" + orderId + '\'' + 
 ", customer='" + customer + '\'' + 
 ", status=" + status + 
 ", amount=" + amount + 
 ", date=" + date + 
 '}'; 
 } 
}

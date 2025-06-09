package com.example.personalized;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class SparkPi {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("PersonalizedPageRank")
                .master("local[*]")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // doc du lieu do thi
        String filePath = "src/data/graph.txt";  //
        Map<String, List<String>> links = new HashMap<>();

        try {
            List<String> lines = Files.readAllLines(Paths.get(filePath));
            for (String line : lines) {
                line = line.trim();
                if (line.isEmpty()) continue;

                String[] parts = line.split("\\s+");
                String node = parts[0];

                List<String> neighbors = new ArrayList<>();
                if (parts.length > 1) {
                    neighbors = Arrays.asList(Arrays.copyOfRange(parts, 1, parts.length));
                }

                links.put(node, neighbors);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        List<Tuple2<String, List<String>>> linkTuples = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : links.entrySet()) {
            linkTuples.add(new Tuple2<>(entry.getKey(), entry.getValue()));
        }
        JavaPairRDD<String, List<String>> linksRDD = sc.parallelizePairs(linkTuples).cache();

        // Personalized source
        String source = "p4";

        // Khởi tạo ranks
        Map<String, Double> initialRanksMap = new HashMap<>();
        for (String page : links.keySet()) {
            initialRanksMap.put(page, page.equals(source) ? 1.0  : 0);
        }

        List<Tuple2<String, Double>> rankTuples = new ArrayList<>();
        for (Map.Entry<String, Double> entry : initialRanksMap.entrySet()) {
            rankTuples.add(new Tuple2<>(entry.getKey(), entry.getValue()));
        }
        JavaPairRDD<String, Double> ranks = sc.parallelizePairs(rankTuples);

        double damping = 0.85;
        double teleport = (1 - damping);
        int maxIter = 100;
        double epsilon = 1e-6;
        int numNodes = links.size();

        for (int iter = 0; iter < maxIter; iter++) {
            JavaPairRDD<String, Double> prevRanks = ranks;

            // Join links với ranks để phát tán rank
            JavaPairRDD<String, Tuple2<List<String>, Double>> linksWithRanks = linksRDD.join(prevRanks);

            // Tính đóng góp rank
            JavaPairRDD<String, Double> contrib = linksWithRanks.flatMapToPair(t -> {
                String page = t._1;
                List<String> neighbors = t._2._1;
                Double rank = t._2._2;

                List<Tuple2<String, Double>> results = new ArrayList<>();

                if (neighbors.isEmpty()) {
                    double share = rank / numNodes;
                    for (String otherPage : links.keySet()) {
                        results.add(new Tuple2<>(otherPage, share));
                    }
                } else {
                    double share = rank / neighbors.size();
                    for (String nbr : neighbors) {
                        results.add(new Tuple2<>(nbr, share));
                    }
                }
                return results.iterator();
            });

            // Cộng dồn rank và áp dụng damping + teleport
            JavaPairRDD<String, Double> newRanks = contrib
                    .reduceByKey(Double::sum)
                    .mapValues(sum -> damping * sum);

            // Thêm phần teleport cho node nguồn
            JavaPairRDD<String, Double> newRanksWithTeleport = newRanks.mapToPair(t -> {
                if (t._1.equals(source)) {
                    return new Tuple2<>(t._1, t._2 + teleport);
                } else {
                    return new Tuple2<>(t._1, t._2);
                }
            });

            ranks = linksRDD.keys().mapToPair(page -> new Tuple2<>(page, 0.0))
                    .leftOuterJoin(newRanksWithTeleport)
                    .mapToPair(t -> new Tuple2<>(t._1, t._2._2.orElse(t._2._1)));

            // Kiểm tra hội tụ
            Map<String, Double> currMap = ranks.collectAsMap();
            Map<String, Double> prevMap = prevRanks.collectAsMap();

            double diff = 0.0;
            for (String page : currMap.keySet()) {
                diff += Math.abs(currMap.get(page) - prevMap.getOrDefault(page, 0.0));
            }

            System.out.printf("Iteration %d: total diff = %.10f%n", iter + 1, diff);
            if (diff < epsilon) {
                System.out.println("=> da hoi tu sau  " + (iter + 1) + " vong lap.");
                break;
            }
        }

        // Hiển thị kết quả cuối cùng
        System.out.println("\nPersonalized PageRank scores:");
        Map<String, Double> finalRanks = ranks.collectAsMap();
        finalRanks.forEach((page, rank) -> System.out.println(page + " : " + rank));

        sc.close();
        spark.close();
    }
}

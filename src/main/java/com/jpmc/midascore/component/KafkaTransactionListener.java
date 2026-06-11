package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Optional;

@Component
public class KafkaTransactionListener {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    private final RestTemplate restTemplate = new RestTemplate();

    @KafkaListener(topics = "${general.kafka-topic}")
    public void listen(Transaction transaction) {
        Optional<UserRecord> senderOpt = userRepository.findById(transaction.getSenderId());
        Optional<UserRecord> recipientOpt = userRepository.findById(transaction.getRecipientId());

        if (senderOpt.isPresent() && recipientOpt.isPresent()) {
            UserRecord sender = senderOpt.get();
            UserRecord recipient = recipientOpt.get();

            if (sender.getBalance() >= transaction.getAmount()) {

                Incentive incentive = restTemplate.postForObject(
                        "http://localhost:8081/incentive",
                        transaction,
                        Incentive.class);
                float incentiveAmount = (incentive != null) ? incentive.getAmount() : 0;

                sender.setBalance(sender.getBalance() - transaction.getAmount());
                recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

                userRepository.save(sender);
                userRepository.save(recipient);

                TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount(),
                        incentiveAmount);
                transactionRepository.save(record);

                System.out.println("Saved transaction: " + transaction + " incentive: " + incentiveAmount);

                userRepository.findAll().forEach(user -> {
                    if (user.getName().equals("wilbur")) {
                        System.out.println("WILBUR BALANCE: " + user.getBalance());
                    }
                });
            }
        }
    }
}
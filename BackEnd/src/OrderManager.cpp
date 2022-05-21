//
// Created by Rainy Memory on 2021/4/14.
//

#include "OrderManager.h"

void OrderManager::outputSuccess(long long message) {
    if (!ioctl::disablePrint) defaultOut << message << endl;
}

void OrderManager::outputFailure() {
    if (!ioctl::disablePrint) defaultOut << "-1" << endl;
}

void OrderManager::outputQueue() {
    if (!ioctl::disablePrint) defaultOut << "queue" << endl;
}

void OrderManager::printOrder(const OrderManager::order_t &o) {
    if (!ioctl::disablePrint) defaultOut << status[o.status] << " " << o.trainID << " " << o.fromStation << " " << o.departureTime << " -> " << o.toStation << " " << o.arrivalTime << " " << o.price << " " << o.num << endl;
}

void OrderManager::buyTicket(const Parser &p) {
    if (!userManager->isLogin(p["-u"]))return outputFailure();
    hash_t hashedTrainID {trainManager->hashTrainID(p["-i"])};
    std::pair<int, bool> temp {trainManager->indexPool.find(hashedTrainID)};
    if (!temp.second)return outputFailure();
    train_t targetTrain {trainManager->storagePool.read(temp.first)};
    if (!targetTrain.released)return outputFailure();
    int n = p("-n");
    if (targetTrain.seatNum < n)return outputFailure();
    train_time_t departureDate {(p["-d"][0] - '0') * 10 + p["-d"][1] - '0', (p["-d"][3] - '0') * 10 + p["-d"][4] - '0', 0, 0};
    int from = -1, to = -1;
    for (int i = 0; i < targetTrain.stationNum; i++) {
        if (targetTrain.stations[i] == p["-f"])from = i;
        if (targetTrain.stations[i] == p["-t"])to = i;
    }
    if (from == -1 || to == -1 || from >= to)return outputFailure();
    train_time_t dTimes {targetTrain.departureTimes[from]};
    if (!(dTimes.lessOrEqualDate(departureDate) && departureDate.lessOrEqualDate(dTimes.updateDate(targetTrain.dateGap))))return outputFailure();
    int dist = departureDate.dateDistance(targetTrain.departureTimes[from]);
    hash_t hashedUsername {userManager->hashUsername(p["-u"])};
    std::pair<hash_t, int> key {hashedTrainID, dist};
#ifndef storageTicketData
    std::pair<int, bool> ticketTemp {trainManager->ticketPool.find(key)};
    date_ticket_t seats {trainManager->ticketStoragePool.read(ticketTemp.first)};
#else
    std::pair<date_ticket_t, bool> ticketTemp {trainManager->ticketPool.find(key)};
    date_ticket_t &seats {ticketTemp.first};
#endif
    int num = seats.ticketNum(from, to);
    int price = targetTrain.prices[to] - targetTrain.prices[from];
    bool queue = num < n, candidate = p.haveThisArgument("-q") && p["-q"] == "true";
    if (queue) {
        if (!candidate)return outputFailure();
        order_t order {indexPool.size(), p["-u"], PENDING, targetTrain.trainID, targetTrain.stations[from], targetTrain.stations[to],
                       targetTrain.departureTimes[from].updateDate(dist), targetTrain.arrivalTimes[to].updateDate(dist), price, n, from, to, dist};
        indexPool.insert(hashedUsername, order.orderID, order);
        pending_order_t pOrder {hashedUsername, order.orderID, order.from, order.to, order.num};
        pendingPool.insert(std::pair<hash_t, int> {hashedTrainID, dist}, order.orderID, pOrder);
        return outputQueue();
    }
    seats.modifyRemain(from, to, -n);
    trainManager->storagePool.update(targetTrain, temp.first);
#ifndef storageTicketData
    trainManager->ticketStoragePool.update(seats, ticketTemp.first);
#else
    trainManager->ticketPool.update(key, seats);
#endif
    order_t order {indexPool.size(), p["-u"], SUCCESS, targetTrain.trainID, targetTrain.stations[from], targetTrain.stations[to],
                   targetTrain.departureTimes[from].updateDate(dist), targetTrain.arrivalTimes[to].updateDate(dist), price, n, from, to, dist};
    indexPool.insert(hashedUsername, order.orderID, order);
    outputSuccess((long long) price * (long long) n);
}

void OrderManager::queryOrder(const Parser &p) {
    if (!userManager->isLogin(p["-u"]))return outputFailure();
    static vector<order_t> result;
    result.clear();
    indexPool.find(userManager->hashUsername(p["-u"]), result);
    if (!ioctl::disablePrint) defaultOut << result.size() << endl;
    for (const order_t &i : result)printOrder(i);
}

void OrderManager::refundTicket(const Parser &p) {
    if (!userManager->isLogin(p["-u"]))return outputFailure();
    int n = p.haveThisArgument("-n") ? p("-n") : 1;
    hash_t hashedUsername {userManager->hashUsername(p["-u"])};
    std::pair<order_t, bool> o = indexPool.findNth(hashedUsername, n);
    if (!o.second)return outputFailure();
    order_t &rOrder {o.first};
    if (rOrder.status == REFUNDED)return outputFailure();
    hash_t hashedTrainID {trainManager->hashTrainID(rOrder.trainID)};
    bool newTicket = rOrder.status == SUCCESS;
    rOrder.status = REFUNDED;
    indexPool.update(hashedUsername, rOrder.orderID, rOrder);
    if (!newTicket) {
        pendingPool.erase(std::pair<hash_t, int> {hashedTrainID, rOrder.dist}, rOrder.orderID);
        return outputSuccess();
    }
    std::pair<hash_t, int> key {hashedTrainID, rOrder.dist};
#ifndef storageTicketData
    std::pair<int, bool> ticketTemp {trainManager->ticketPool.find(key)};
    date_ticket_t seats {trainManager->ticketStoragePool.read(ticketTemp.first)};
#else
    std::pair<date_ticket_t, bool> ticketTemp {trainManager->ticketPool.find(key)};
    date_ticket_t &seats {ticketTemp.first};
#endif
    seats.modifyRemain(rOrder.from, rOrder.to, rOrder.num);
    static vector<pending_order_t> pOrder;
    pOrder.clear();
    pendingPool.find(std::pair<hash_t, int> {hashedTrainID, rOrder.dist}, pOrder);
    int num;
    for (int i = pOrder.size() - 1; i >= 0; i--) {
        pending_order_t &k = pOrder[i];
        if (k.to < rOrder.from || rOrder.to < k.from)continue;
        num = seats.ticketNum(k.from, k.to);
        if (num < k.num)continue;
        seats.modifyRemain(k.from, k.to, -k.num);
        indexPool.updateFirstMember(k.hashedUsername, k.orderID, SUCCESS);
        pendingPool.erase(std::pair<hash_t, int> {hashedTrainID, rOrder.dist}, k.orderID);
    }
#ifndef storageTicketData
    trainManager->ticketStoragePool.update(seats, ticketTemp.first);
#else
    trainManager->ticketPool.update(key, seats);
#endif
    outputSuccess();
}

void OrderManager::rollback(int timeStamp) {
    indexPool.rollback(timeStamp);
    pendingPool.rollback(timeStamp);
}

void OrderManager::clear() {
    indexPool.clear();
    pendingPool.clear();
}

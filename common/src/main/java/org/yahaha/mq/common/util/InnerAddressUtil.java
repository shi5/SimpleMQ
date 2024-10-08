package org.yahaha.mq.common.util;

import com.github.houbb.heaven.util.common.ArgUtil;
import org.yahaha.mq.common.rpc.RpcAddress;

import java.util.ArrayList;
import java.util.List;

public class InnerAddressUtil {
    private InnerAddressUtil(){}

    /**
     * 初始化地址信息
     * @param address 地址
     * @return 结果列表
     *      */
    public static List<RpcAddress> initAddressList(String address) {
        ArgUtil.notEmpty(address, "address");

        String[] strings = address.split(",");
        List<RpcAddress> list = new ArrayList<>();
        for(String s : strings) {
            String[] infos = s.split(":");

            RpcAddress rpcAddress = new RpcAddress();
            rpcAddress.setAddress(infos[0]);
            rpcAddress.setPort(Integer.parseInt(infos[1]));
            if(infos.length > 2) {
                rpcAddress.setWeight(Integer.parseInt(infos[2]));
            }
            list.add(rpcAddress);
        }
        return list;
    }

}

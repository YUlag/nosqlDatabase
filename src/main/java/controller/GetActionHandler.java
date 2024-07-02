package controller;

import dto.ActionDTO;
import dto.RespDTO;
import dto.RespStatusTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import service.Store;
import utils.LoggerUtil;

import java.io.IOException;
import java.io.ObjectOutputStream;

public class GetActionHandler implements ActionHandler {
    private final Logger LOGGER = LoggerFactory.getLogger(GetActionHandler.class);
    private Store store;

    public GetActionHandler(Store store) {
        this.store = store;
    }

    @Override
    public void handleAction(ActionDTO dto, ObjectOutputStream oos) throws IOException {
        String value = store.get(dto.getKey());
        LoggerUtil.debug(LOGGER, "[SocketServerHandler][run]: {}", "get action resp" + dto.toString());
        RespDTO resp = new RespDTO(RespStatusTypeEnum.SUCCESS, value);
        oos.writeObject(resp);
        oos.flush();
    }
}

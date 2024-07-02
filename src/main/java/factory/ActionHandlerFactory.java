package factory;

import controller.*;
import dto.ActionTypeEnum;
import service.Store;

import java.util.HashMap;
import java.util.Map;

public class ActionHandlerFactory {
    private Map<ActionTypeEnum, ActionHandler> handlerMap = new HashMap<>();

    public ActionHandlerFactory(Store store) {
        handlerMap.put(ActionTypeEnum.GET, new GetActionHandler(store));
        handlerMap.put(ActionTypeEnum.SET, new SetActionHandler(store));
        handlerMap.put(ActionTypeEnum.RM, new RmActionHandler(store));
        handlerMap.put(ActionTypeEnum.CLOSE,new CloseActionHandler(store));
    }

    public ActionHandler getHandler(ActionTypeEnum actionType) {
        return handlerMap.get(actionType);
    }
}

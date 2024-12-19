import { makeAutoObservable } from 'mobx';

export type optimizationResult = {
    Optimization: string | number;
    Portfolio_Return: string | number;
    Portfolio_Risk: string | number;
    Stocks: { name: string; value: string }[];
};

class Store {
    currentPage: 'greeting' | 'chooseCurrency' | 'freeOptimization' | 'result' | 'paidOptimization' | 'clientOptimization' = 'greeting';
    currentOptimizationType: 'free' | 'paid' = 'free';
    currencys: string[] = [];
    currency: string = '';
    resultWeight: optimizationResult[] = [];
    resultQuantity: optimizationResult[] = [];
    isClientOptimization: boolean = false;
    imageFigure: string = '';

    constructor() {
        makeAutoObservable(this);
    }

    get getCurrencys() {
        return this.currencys;
    }

    set setCurrencys(data: string[]) {
        this.currencys = data;
    }

    get getCurrency() {
        return this.currency;
    }

    set setCurrency(data: string) {
        this.currency = data;
    }

    get getCurrentPage() {
        return this.currentPage;
    }

    set setCurrentPage(data: 'greeting' | 'chooseCurrency' | 'freeOptimization' | 'result' | 'paidOptimization' | 'clientOptimization') {
        this.currentPage = data;
    }

    get getResultWeight() {
        return this.resultWeight;
    }

    set setResultWeight(data: optimizationResult[]) {
        this.resultWeight = data;
    }

    get getResultQuantity() {
        return this.resultQuantity;
    }

    set setResultQuantity(data: optimizationResult[]) {
        this.resultQuantity = data;
    }

    get getCurrentOptimizationType() {
        return this.currentOptimizationType;
    }

    set setCurrentOptimizationType(data: 'free' | 'paid') {
        this.currentOptimizationType = data;
    }

    get getIsClientOptimization() {
        return this.isClientOptimization;
    }

    set setIsClientOptimization(data: boolean) {
        this.isClientOptimization = data;
    }

    get getImageFigure() {
        return this.imageFigure;
    }

    set setImageFigure(data: string) {
        this.imageFigure = data;
    }
}

export default new Store();
